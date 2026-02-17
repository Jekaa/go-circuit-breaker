package circuitbreaker

import (
	"container/ring"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
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

// Определяем ошибки как переменные пакета
var (
	ErrCircuitOpen       = errors.New("circuit breaker is open")
	ErrHalfOpenRateLimit = errors.New("half-open: rate limit exceeded")
	ErrRequestTimeout    = errors.New("request timeout")
)

// Внутренние интерфейсы (определены потребителем)
type cache interface {
	Get(interface{}) (interface{}, bool)
	Set(interface{}, interface{})
}

type metricsCollector interface {
	RecordError()
	RecordSuccess()
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

	// Коэффициент адаптивности порога (0-1)
	AdaptiveThresholdFactor float64

	// FallbackCache для graceful degradation
	FallbackCache cache

	// Сборщик метрик для адаптивного порога
	MetricsCollector metricsCollector
}

// DefaultConfig возвращает конфигурацию по умолчанию
func DefaultConfig() Config {
	return Config{
		ErrorThreshold:          50.0,
		OpenTimeout:             30 * time.Second,
		WindowSize:              100,
		MinRequests:             10,
		MaxConcurrentRequests:   5,
		AdaptiveThresholdFactor: 0.3,
	}
}

// slidingWindow реализует скользящее окно с атомарными операциями
type slidingWindow struct {
	values []int64
	index  uint64
	size   int
}

func newSlidingWindow(size int) *slidingWindow {
	return &slidingWindow{
		values: make([]int64, size),
		size:   size,
	}
}

func (sw *slidingWindow) Add(value int64) {
	idx := atomic.AddUint64(&sw.index, 1) % uint64(sw.size)
	atomic.StoreInt64(&sw.values[idx], value)
}

func (sw *slidingWindow) Sum() int64 {
	var sum int64
	for i := 0; i < sw.size; i++ {
		sum += atomic.LoadInt64(&sw.values[i])
	}
	return sum
}

// CircuitBreaker реализует паттерн автоматического выключателя
type CircuitBreaker struct {
	// Атомарные поля должны быть выравнены для 64-битных архитектур
	state     int32
	failures  int64
	successes int64

	config        Config
	failureWindow unsafe.Pointer // *slidingWindow

	// Таймер для открытого состояния
	openExpiry atomic.Value // time.Time

	// Управление Half-Open состоянием
	requestCount int64 // атомарный счетчик запросов в Half-Open

	// Метрики для адаптивного порога
	historicalRates *ring.Ring

	mu sync.RWMutex

	// Канал для оповещения о смене состояния
	stateChange chan State

	// Контекст для управления жизненным циклом
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewCircuitBreaker создает новый экземпляр Circuit Breaker
func NewCircuitBreaker(config Config) *CircuitBreaker {
	ctx, cancel := context.WithCancel(context.Background())

	cb := &CircuitBreaker{
		config:          config,
		historicalRates: ring.New(24), // храним 24 часа метрик
		stateChange:     make(chan State, 1),
		cancel:          cancel,
	}

	// Инициализируем окно ошибок
	window := newSlidingWindow(config.WindowSize)
	atomic.StorePointer(&cb.failureWindow, unsafe.Pointer(window))

	// Инициализируем openExpiry нулевым временем
	cb.openExpiry.Store(time.Time{})

	// Запускаем фоновые горутины
	cb.wg.Add(1)
	go cb.monitorOpenState(ctx)

	return cb
}

// Stop останавливает фоновые горутины
func (cb *CircuitBreaker) Stop() {
	cb.cancel()
	cb.wg.Wait()
}

// State возвращает текущее состояние выключателя с проверкой таймаута
func (cb *CircuitBreaker) State() State {
	state := State(atomic.LoadInt32(&cb.state))

	// Проверяем истечение таймера для Open состояния
	if state == StateOpen {
		expiry := cb.openExpiry.Load().(time.Time)
		if !expiry.IsZero() && time.Now().After(expiry) {
			// Пытаемся переключиться в HalfOpen
			if atomic.CompareAndSwapInt32(&cb.state, int32(StateOpen), int32(StateHalfOpen)) {
				cb.notifyStateChange(StateHalfOpen)
				return StateHalfOpen
			}
			// Если не удалось, читаем актуальное состояние
			return State(atomic.LoadInt32(&cb.state))
		}
	}

	return state
}

// setState атомарно меняет состояние
func (cb *CircuitBreaker) setState(newState State) {
	old := State(atomic.SwapInt32(&cb.state, int32(newState)))
	if old != newState {
		cb.notifyStateChange(newState)
	}
}

func (cb *CircuitBreaker) notifyStateChange(state State) {
	select {
	case cb.stateChange <- state:
	default:
	}
}

// monitorOpenState отслеживает состояние Open и переключает в HalfOpen
func (cb *CircuitBreaker) monitorOpenState(ctx context.Context) {
	defer cb.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			state := State(atomic.LoadInt32(&cb.state))
			if state == StateOpen {
				expiry := cb.openExpiry.Load().(time.Time)
				if !expiry.IsZero() && time.Now().After(expiry) {
					atomic.CompareAndSwapInt32(&cb.state, int32(StateOpen), int32(StateHalfOpen))
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// Execute выполняет запрос через Circuit Breaker
func (cb *CircuitBreaker) Execute(ctx context.Context, req interface{}) (interface{}, error) {
	state := cb.State()

	switch state {
	case StateOpen:
		// Graceful degradation: пробуем вернуть закэшированный ответ
		if cb.config.FallbackCache != nil {
			if resp, ok := cb.config.FallbackCache.Get(req); ok {
				return resp, nil
			}
		}
		return nil, ErrCircuitOpen

	case StateHalfOpen:
		// Лимитируем количество одновременных запросов в HalfOpen
		count := atomic.AddInt64(&cb.requestCount, 1)
		defer atomic.AddInt64(&cb.requestCount, -1)

		if count > cb.config.MaxConcurrentRequests {
			if cb.config.FallbackCache != nil {
				if resp, ok := cb.config.FallbackCache.Get(req); ok {
					return resp, nil
				}
			}
			return nil, ErrHalfOpenRateLimit
		}

		// Выполняем запрос в HalfOpen
		resp, err := cb.doRequest(ctx, req)
		cb.recordHalfOpenResult(err)
		return resp, err

	case StateClosed:
		fallthrough
	default:
		// В закрытом состоянии выполняем запрос
		resp, err := cb.doRequest(ctx, req)
		cb.recordResult(err)
		return resp, err
	}
}

// recordHalfOpenResult анализирует результат в HalfOpen состоянии
func (cb *CircuitBreaker) recordHalfOpenResult(err error) {
	if err == nil {
		successes := atomic.AddInt64(&cb.successes, 1)
		// Если достаточно успехов, закрываем выключатель
		if successes >= int64(cb.config.MinRequests/2) {
			cb.reset()
		}
	} else {
		// Любая ошибка в HalfOpen возвращает в Open
		failures := atomic.AddInt64(&cb.failures, 1)
		if failures >= 1 {
			cb.open()
		}
	}

	// Обновляем метрики
	if cb.config.MetricsCollector != nil && err != nil {
		cb.config.MetricsCollector.RecordError()
	}
}

// recordResult анализирует результат запроса в Closed состоянии
func (cb *CircuitBreaker) recordResult(err error) {
	window := (*slidingWindow)(atomic.LoadPointer(&cb.failureWindow))

	if err == nil {
		atomic.AddInt64(&cb.successes, 1)
		window.Add(0)
	} else {
		atomic.AddInt64(&cb.failures, 1)
		window.Add(1)

		// Обновляем метрики
		if cb.config.MetricsCollector != nil {
			cb.config.MetricsCollector.RecordError()
		}

		// Асинхронная проверка порога
		go cb.checkThresholdAsync()
	}
}

// checkThresholdAsync асинхронно проверяет порог ошибок
func (cb *CircuitBreaker) checkThresholdAsync() {
	if State(atomic.LoadInt32(&cb.state)) != StateClosed {
		return
	}

	window := (*slidingWindow)(atomic.LoadPointer(&cb.failureWindow))
	failures := window.Sum()
	total := atomic.LoadInt64(&cb.successes) + failures

	if total < int64(cb.config.MinRequests) {
		return
	}

	errorRate := float64(failures) / float64(total) * 100

	// Адаптивный порог
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
	// Пытаемся переключиться в Open, только если не в Open уже
	if !atomic.CompareAndSwapInt32(&cb.state, int32(StateClosed), int32(StateOpen)) {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.openExpiry.Store(time.Now().Add(cb.config.OpenTimeout))

	// Сбрасываем счетчики
	atomic.StoreInt64(&cb.successes, 0)
	atomic.StoreInt64(&cb.failures, 0)

	cb.notifyStateChange(StateOpen)
}

// reset закрывает выключатель (возвращает в нормальное состояние)
func (cb *CircuitBreaker) reset() {
	if !atomic.CompareAndSwapInt32(&cb.state, int32(StateHalfOpen), int32(StateClosed)) {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Создаем новое окно ошибок
	newWindow := newSlidingWindow(cb.config.WindowSize)
	atomic.StorePointer(&cb.failureWindow, unsafe.Pointer(newWindow))

	atomic.StoreInt64(&cb.successes, 0)
	atomic.StoreInt64(&cb.failures, 0)

	cb.notifyStateChange(StateClosed)
}

// doRequest имитирует выполнение запроса
func (cb *CircuitBreaker) doRequest(ctx context.Context, req interface{}) (interface{}, error) {
	select {
	case <-ctx.Done():
		return nil, ErrRequestTimeout
	case <-time.After(100 * time.Millisecond):
		// В реальном коде здесь будет вызов API
		return "success", nil
	}
}

// Metrics содержит метрики Circuit Breaker
type Metrics struct {
	State            State
	Failures         int64
	Successes        int64
	ErrorRate        float64
	OpenExpiry       time.Time
	HalfOpenRequests int64
}

// GetMetrics возвращает текущие метрики выключателя
func (cb *CircuitBreaker) GetMetrics() Metrics {
	window := (*slidingWindow)(atomic.LoadPointer(&cb.failureWindow))
	failures := window.Sum()
	successes := atomic.LoadInt64(&cb.successes)
	total := successes + failures

	var errorRate float64
	if total > 0 {
		errorRate = float64(failures) / float64(total) * 100
	}

	expiry := cb.openExpiry.Load().(time.Time)

	return Metrics{
		State:            cb.State(),
		Failures:         failures,
		Successes:        successes,
		ErrorRate:        errorRate,
		OpenExpiry:       expiry,
		HalfOpenRequests: atomic.LoadInt64(&cb.requestCount),
	}
}

// GenericCircuitBreaker - обобщенная версия с типами
type GenericCircuitBreaker[T any, R any] struct {
	*CircuitBreaker
	executor func(context.Context, T) (R, error)
}

// NewGenericCircuitBreaker создает типизированный Circuit Breaker
func NewGenericCircuitBreaker[T any, R any](
	config Config,
	executor func(context.Context, T) (R, error),
) *GenericCircuitBreaker[T, R] {
	return &GenericCircuitBreaker[T, R]{
		CircuitBreaker: NewCircuitBreaker(config),
		executor:       executor,
	}
}

// Execute выполняет типизированный запрос
func (gcb *GenericCircuitBreaker[T, R]) Execute(ctx context.Context, req T) (R, error) {
	var zero R

	state := gcb.State()

	switch state {
	case StateOpen:
		if gcb.config.FallbackCache != nil {
			if resp, ok := gcb.config.FallbackCache.Get(req); ok {
				if typedResp, ok := resp.(R); ok {
					return typedResp, nil
				}
			}
		}
		return zero, ErrCircuitOpen

	case StateHalfOpen:
		count := atomic.AddInt64(&gcb.requestCount, 1)
		defer atomic.AddInt64(&gcb.requestCount, -1)

		if count > gcb.config.MaxConcurrentRequests {
			if gcb.config.FallbackCache != nil {
				if resp, ok := gcb.config.FallbackCache.Get(req); ok {
					if typedResp, ok := resp.(R); ok {
						return typedResp, nil
					}
				}
			}
			return zero, ErrHalfOpenRateLimit
		}

		resp, err := gcb.executor(ctx, req)
		if err == nil {
			if gcb.config.FallbackCache != nil {
				gcb.config.FallbackCache.Set(req, resp)
			}
		}
		gcb.recordHalfOpenResult(err)
		return resp, err

	default:
		resp, err := gcb.executor(ctx, req)
		if err == nil {
			if gcb.config.FallbackCache != nil {
				gcb.config.FallbackCache.Set(req, resp)
			}
		}
		gcb.recordResult(err)
		return resp, err
	}
}
