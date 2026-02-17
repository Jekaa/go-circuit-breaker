package circuitbreaker

import (
	"container/ring"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

// State представляет состояние Circuit Breaker
type State int32

const (
	// StateClosed - нормальное состояние, запросы проходят
	StateClosed State = iota
	// StateOpen - аварийное состояние, запросы блокируются
	StateOpen
	// StateHalfOpen - состояние проверки восстановления
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Request представляет запрос к внешнему сервису
type Request interface{}

// Response представляет ответ от внешнего сервиса
type Response interface{}

// FallbackCache предоставляет интерфейс для хранения fallback ответов
type FallbackCache interface {
	// Get возвращает закэшированный ответ для запроса
	Get(req Request) (Response, bool)
	// Set сохраняет успешный ответ в кэш
	Set(req Request, resp Response)
}

// MetricsCollector собирает метрики для адаптивного порога
type MetricsCollector interface {
	// RecordError записывает ошибку
	RecordError()
	// RecordSuccess записывает успех
	RecordSuccess()
	// GetErrorRate возвращает среднюю частоту ошибок за последний час
	GetErrorRate() float64
}

// Config содержит конфигурацию Circuit Breaker
type Config struct {
	// Имя выключателя для логирования и метрик
	Name string

	// Порог ошибок для перехода в Open (в процентах, 0-100)
	ErrorThreshold float64

	// Время нахождения в Open состоянии
	OpenTimeout time.Duration

	// Размер скользящего окна для подсчета ошибок
	WindowSize int

	// Минимальное количество запросов для принятия решения
	MinRequests int

	// Максимальное количество одновременных запросов в HalfOpen
	MaxConcurrentRequests int64

	// Размер bucket для токенов в HalfOpen
	HalfOpenBucketSize int

	// Скорость восстановления токенов (токенов в секунду)
	HalfOpenRefillRate float64

	// Коэффициент адаптивности порога (0-1)
	AdaptiveThresholdFactor float64

	// FallbackCache для graceful degradation
	FallbackCache FallbackCache

	// Сборщик метрик для адаптивного порога
	MetricsCollector MetricsCollector
}

// DefaultConfig возвращает конфигурацию по умолчанию
func DefaultConfig() Config {
	return Config{
		ErrorThreshold:          50.0,
		OpenTimeout:             30 * time.Second,
		WindowSize:              100,
		MinRequests:             10,
		MaxConcurrentRequests:   5,
		HalfOpenBucketSize:      10,
		HalfOpenRefillRate:      2.0,
		AdaptiveThresholdFactor: 0.3,
	}
}

// slidingWindow реализует скользящее окно для подсчета событий
type slidingWindow struct {
	mu      sync.Mutex
	ring    *ring.Ring
	size    int
	current int64
}

func newSlidingWindow(size int) *slidingWindow {
	return &slidingWindow{
		ring: ring.New(size),
		size: size,
	}
}

func (sw *slidingWindow) Add(value int64) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.ring.Value = value
	sw.ring = sw.ring.Next()
}

func (sw *slidingWindow) Sum() int64 {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	var sum int64
	sw.ring.Do(func(v interface{}) {
		if val, ok := v.(int64); ok {
			sum += val
		}
	})
	return sum
}

// CircuitBreaker реализует паттерн автоматического выключателя
// с адаптивным порогом и токен-бакетом для Half-Open состояния.
type CircuitBreaker struct {
	// Атомарные поля должны быть выравнены для 64-битных архитектур
	state     int32
	failures  int64
	successes int64

	config        Config
	failureWindow *slidingWindow

	// Таймер для открытого состояния
	openTimer  *time.Timer
	openExpiry time.Time

	// Управление Half-Open состоянием
	semaphore   *semaphore.Weighted
	tokenBucket chan struct{}
	lastRefill  time.Time

	// Метрики для адаптивного порога
	historicalRates *ring.Ring

	mu sync.RWMutex

	// Канал для оповещения о смене состояния
	stateChange chan State
}

// NewCircuitBreaker создает новый экземпляр Circuit Breaker
func NewCircuitBreaker(config Config) *CircuitBreaker {
	cb := &CircuitBreaker{
		config:          config,
		failureWindow:   newSlidingWindow(config.WindowSize),
		semaphore:       semaphore.NewWeighted(config.MaxConcurrentRequests),
		tokenBucket:     make(chan struct{}, config.HalfOpenBucketSize),
		historicalRates: ring.New(24), // храним 24 часа метрик
		stateChange:     make(chan State, 1),
	}

	// Инициализируем токен бакет
	for i := 0; i < config.HalfOpenBucketSize; i++ {
		cb.tokenBucket <- struct{}{}
	}
	cb.lastRefill = time.Now()

	// Запускаем горутину для refill токенов
	go cb.refillTokenBucket()

	return cb
}

// State возвращает текущее состояние выключателя (атомарно)
func (cb *CircuitBreaker) State() State {
	return State(atomic.LoadInt32(&cb.state))
}

// setState атомарно меняет состояние
func (cb *CircuitBreaker) setState(newState State) {
	old := State(atomic.SwapInt32(&cb.state, int32(newState)))
	if old != newState {
		select {
		case cb.stateChange <- newState:
		default:
		}
	}
}

// Execute выполняет запрос через Circuit Breaker
func (cb *CircuitBreaker) Execute(ctx context.Context, req Request) (Response, error) {
	// Проверяем состояние перед выполнением
	switch cb.State() {
	case StateOpen:
		// Проверяем, не пора ли перейти в Half-Open
		if time.Now().After(cb.openExpiry) {
			cb.setState(StateHalfOpen)
		} else {
			// Graceful degradation: пробуем вернуть закэшированный ответ
			if cb.config.FallbackCache != nil {
				if resp, ok := cb.config.FallbackCache.Get(req); ok {
					return resp, nil
				}
			}
			return nil, errors.New("circuit breaker is open")
		}
	case StateHalfOpen:
		// В Half-Open используем семафор и токен бакет для контроля
		if err := cb.acquireHalfOpenToken(ctx); err != nil {
			// Не можем получить токен или семафор - возвращаем fallback
			if cb.config.FallbackCache != nil {
				if resp, ok := cb.config.FallbackCache.Get(req); ok {
					return resp, nil
				}
			}
			return nil, errors.New("half-open: rate limit exceeded")
		}
		defer cb.releaseHalfOpenToken()
	case StateClosed:
		// В закрытом состоянии запросы выполняются без ограничений
		// Просто выполняем запрос и записываем результат
		resp, err := cb.doRequest(ctx, req)
		cb.recordResult(err)
		return resp, err
	}

	// Выполняем запрос (здесь должна быть реальная бизнес-логика)
	resp, err := cb.doRequest(ctx, req)

	// Обрабатываем результат
	cb.recordResult(err)

	return resp, err
}

// acquireHalfOpenToken пытается получить доступ в Half-Open состоянии
func (cb *CircuitBreaker) acquireHalfOpenToken(ctx context.Context) error {
	// Сначала пробуем взять семафор (конкурентность)
	if err := cb.semaphore.Acquire(ctx, 1); err != nil {
		return err
	}

	// Затем пробуем взять токен из бакета (rate limiting)
	select {
	case <-cb.tokenBucket:
		return nil
	case <-ctx.Done():
		cb.semaphore.Release(1)
		return ctx.Err()
	default:
		cb.semaphore.Release(1)
		return errors.New("no tokens available")
	}
}

// releaseHalfOpenToken освобождает ресурсы после запроса в Half-Open
func (cb *CircuitBreaker) releaseHalfOpenToken() {
	cb.semaphore.Release(1)
	// Токен не возвращаем - он восстановится через refill
}

// refillTokenBucket периодически пополняет бакет токенов
func (cb *CircuitBreaker) refillTokenBucket() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		cb.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(cb.lastRefill).Seconds()
		tokensToAdd := int(elapsed * cb.config.HalfOpenRefillRate)

		if tokensToAdd > 0 {
			for i := 0; i < tokensToAdd; i++ {
				select {
				case cb.tokenBucket <- struct{}{}:
				default:
					// Бакет полон, выходим
					break
				}
			}
			cb.lastRefill = now
		}
		cb.mu.Unlock()
	}
}

// recordResult анализирует результат запроса и обновляет состояние
func (cb *CircuitBreaker) recordResult(err error) {
	if err == nil {
		atomic.AddInt64(&cb.successes, 1)
		cb.failureWindow.Add(0)

		// В Half-Open успех может закрыть выключатель
		if cb.State() == StateHalfOpen {
			successes := atomic.LoadInt64(&cb.successes)
			if successes >= int64(cb.config.MinRequests/2) {
				cb.reset()
			}
		}
	} else {
		atomic.AddInt64(&cb.failures, 1)
		cb.failureWindow.Add(1)

		// Обновляем метрики для адаптивного порога
		if cb.config.MetricsCollector != nil {
			cb.config.MetricsCollector.RecordError()
		}

		// Проверяем, не пора ли открыть выключатель
		cb.checkThreshold()
	}
}

// checkThreshold проверяет порог ошибок и при необходимости открывает выключатель
func (cb *CircuitBreaker) checkThreshold() {
	if cb.State() != StateClosed {
		return
	}

	failures := cb.failureWindow.Sum()
	total := atomic.LoadInt64(&cb.successes) + failures

	// Недостаточно данных для принятия решения
	if total < int64(cb.config.MinRequests) {
		return
	}

	errorRate := float64(failures) / float64(total) * 100

	// Адаптивный порог: учитываем исторические данные
	adaptiveThreshold := cb.config.ErrorThreshold
	if cb.config.MetricsCollector != nil {
		historicalRate := cb.config.MetricsCollector.GetErrorRate()
		adaptiveThreshold = cb.config.ErrorThreshold*(1-cb.config.AdaptiveThresholdFactor) +
			historicalRate*cb.config.AdaptiveThresholdFactor
	}

	if errorRate >= adaptiveThreshold {
		cb.open()
	}
}

// open переводит выключатель в открытое состояние
func (cb *CircuitBreaker) open() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.setState(StateOpen)
	cb.openExpiry = time.Now().Add(cb.config.OpenTimeout)

	// Сбрасываем счетчики для Half-Open
	atomic.StoreInt64(&cb.successes, 0)
	atomic.StoreInt64(&cb.failures, 0)
}

// reset закрывает выключатель (возвращает в нормальное состояние)
func (cb *CircuitBreaker) reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.setState(StateClosed)
	atomic.StoreInt64(&cb.successes, 0)
	atomic.StoreInt64(&cb.failures, 0)

	// Очищаем окно ошибок
	cb.failureWindow = newSlidingWindow(cb.config.WindowSize)
}

// doRequest имитирует выполнение запроса (в реальном приложении здесь будет вызов API)
func (cb *CircuitBreaker) doRequest(ctx context.Context, req Request) (Response, error) {
	// Эмуляция работы с внешним сервисом
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(100 * time.Millisecond):
		// В реальном коде здесь будет вызов API
		return "success", nil
	}
}

// Metrics содержит метрики Circuit Breaker
type Metrics struct {
	State           State
	Failures        int64
	Successes       int64
	ErrorRate       float64
	OpenExpiry      time.Time
	AvailableTokens int
}

// GetMetrics возвращает текущие метрики выключателя
func (cb *CircuitBreaker) GetMetrics() Metrics {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	failures := cb.failureWindow.Sum()
	total := atomic.LoadInt64(&cb.successes) + failures

	var errorRate float64
	if total > 0 {
		errorRate = float64(failures) / float64(total) * 100
	}

	return Metrics{
		State:           cb.State(),
		Failures:        failures,
		Successes:       atomic.LoadInt64(&cb.successes),
		ErrorRate:       errorRate,
		OpenExpiry:      cb.openExpiry,
		AvailableTokens: len(cb.tokenBucket),
	}
}
