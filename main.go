package main

import (
	"circuit-breaker/circuitbreaker"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// MockAPIClient имитирует проблемный внешний API
type MockAPIClient struct {
	name         string
	failureRate  float64 // процент ошибок
	slowRate     float64 // процент медленных ответов
	responseTime time.Duration
	mu           sync.RWMutex
	requestCount int64
	successCount int64
	failureCount int64
}

func NewMockAPIClient(name string) *MockAPIClient {
	return &MockAPIClient{
		name:         name,
		failureRate:  0.1,  // 10% ошибок изначально
		slowRate:     0.05, // 5% медленных ответов
		responseTime: 50 * time.Millisecond,
	}
}

func (c *MockAPIClient) SetFailureRate(rate float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failureRate = rate
}

func (c *MockAPIClient) SetSlowRate(rate float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.slowRate = rate
}

func (c *MockAPIClient) Call(ctx context.Context, req string) (string, error) {
	atomic.AddInt64(&c.requestCount, 1)

	c.mu.RLock()
	failureRate := c.failureRate
	slowRate := c.slowRate
	responseTime := c.responseTime
	c.mu.RUnlock()

	// Симуляция таймаута
	select {
	case <-ctx.Done():
		atomic.AddInt64(&c.failureCount, 1)
		return "", ctx.Err()
	default:
	}

	// Симуляция медленного ответа
	if rand.Float64() < slowRate {
		time.Sleep(responseTime * 5)
	}

	// Симуляция ошибки
	if rand.Float64() < failureRate {
		atomic.AddInt64(&c.failureCount, 1)
		return "", fmt.Errorf("API %s error", c.name)
	}

	// Симуляция успешного ответа
	time.Sleep(responseTime)
	atomic.AddInt64(&c.successCount, 1)
	return fmt.Sprintf("Response from %s for: %s", c.name, req), nil
}

func (c *MockAPIClient) GetStats() string {
	reqs := atomic.LoadInt64(&c.requestCount)
	succ := atomic.LoadInt64(&c.successCount)
	fail := atomic.LoadInt64(&c.failureCount)

	var rate float64
	if reqs > 0 {
		rate = float64(fail) / float64(reqs) * 100
	}

	return fmt.Sprintf("Requests: %d, Success: %d, Fail: %d, Error Rate: %.1f%%",
		reqs, succ, fail, rate)
}

// SimpleFallbackCache простая реализация кэша для демонстрации
type SimpleFallbackCache struct {
	mu      sync.RWMutex
	data    map[string]string
	maxSize int
	ttl     time.Duration
	expires map[string]time.Time
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

func NewSimpleFallbackCache(maxSize int, ttl time.Duration) *SimpleFallbackCache {
	cache := &SimpleFallbackCache{
		data:    make(map[string]string),
		maxSize: maxSize,
		ttl:     ttl,
		expires: make(map[string]time.Time),
		stopCh:  make(chan struct{}),
	}

	// Запускаем горутину для очистки устаревших записей
	cache.wg.Add(1)
	go cache.cleanup()

	return cache
}

func (c *SimpleFallbackCache) Stop() {
	close(c.stopCh)
	c.wg.Wait()
}

func (c *SimpleFallbackCache) Get(key interface{}) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	strKey, ok := key.(string)
	if !ok {
		return nil, false
	}

	expiry, ok := c.expires[strKey]
	if !ok || time.Now().After(expiry) {
		return nil, false
	}

	val, ok := c.data[strKey]
	return val, ok
}

func (c *SimpleFallbackCache) Set(key, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	strKey, ok := key.(string)
	if !ok {
		return
	}

	strVal, ok := value.(string)
	if !ok {
		return
	}

	// Если достигнут лимит, удаляем самую старую запись
	if len(c.data) >= c.maxSize {
		var oldestKey string
		var oldestTime time.Time

		for k, exp := range c.expires {
			if oldestTime.IsZero() || exp.Before(oldestTime) {
				oldestKey = k
				oldestTime = exp
			}
		}

		delete(c.data, oldestKey)
		delete(c.expires, oldestKey)
	}

	c.data[strKey] = strVal
	c.expires[strKey] = time.Now().Add(c.ttl)
}

func (c *SimpleFallbackCache) cleanup() {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			for key, expiry := range c.expires {
				if now.After(expiry) {
					delete(c.data, key)
					delete(c.expires, key)
				}
			}
			c.mu.Unlock()
		case <-c.stopCh:
			return
		}
	}
}

// SimpleMetricsCollector простая реализация сборщика метрик
type SimpleMetricsCollector struct {
	mu        sync.RWMutex
	errors    []time.Time
	successes []time.Time
	retention time.Duration
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

func NewSimpleMetricsCollector(retention time.Duration) *SimpleMetricsCollector {
	m := &SimpleMetricsCollector{
		errors:    make([]time.Time, 0),
		successes: make([]time.Time, 0),
		retention: retention,
		stopCh:    make(chan struct{}),
	}

	m.wg.Add(1)
	go m.cleanupLoop()

	return m
}

func (m *SimpleMetricsCollector) Stop() {
	close(m.stopCh)
	m.wg.Wait()
}

func (m *SimpleMetricsCollector) RecordError() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors = append(m.errors, time.Now())
}

func (m *SimpleMetricsCollector) RecordSuccess() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.successes = append(m.successes, time.Now())
}

func (m *SimpleMetricsCollector) GetErrorRate() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	cutoff := now.Add(-m.retention)

	var errors, total int
	for _, t := range m.errors {
		if t.After(cutoff) {
			errors++
		}
	}
	for _, t := range m.successes {
		if t.After(cutoff) {
			total++
		}
	}
	total += errors

	if total == 0 {
		return 0
	}
	return float64(errors) / float64(total) * 100
}

func (m *SimpleMetricsCollector) cleanup() {
	cutoff := time.Now().Add(-m.retention)

	errors := make([]time.Time, 0)
	for _, t := range m.errors {
		if t.After(cutoff) {
			errors = append(errors, t)
		}
	}
	m.errors = errors

	successes := make([]time.Time, 0)
	for _, t := range m.successes {
		if t.After(cutoff) {
			successes = append(successes, t)
		}
	}
	m.successes = successes
}

func (m *SimpleMetricsCollector) cleanupLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.mu.Lock()
			m.cleanup()
			m.mu.Unlock()
		case <-m.stopCh:
			return
		}
	}
}

func main() {
	// Инициализация компонентов
	cache := NewSimpleFallbackCache(100, 5*time.Minute)
	defer cache.Stop()

	metrics := NewSimpleMetricsCollector(1 * time.Hour)
	defer metrics.Stop()

	// Настройка конфигурации Circuit Breaker
	config := circuitbreaker.DefaultConfig()
	config.Name = "main-api"
	config.ErrorThreshold = 30.0 // 30% ошибок для открытия
	config.OpenTimeout = 5 * time.Second
	config.WindowSize = 50
	config.MinRequests = 10
	config.MaxConcurrentRequests = 3
	config.AdaptiveThresholdFactor = 0.3
	config.FallbackCache = cache
	config.MetricsCollector = metrics

	// Создаем Circuit Breaker
	cb := circuitbreaker.NewCircuitBreaker(config)
	defer cb.Stop()

	// Создаем мок API клиента
	api := NewMockAPIClient("payment-service")

	// Демонстрация 1: Нормальная работа
	fmt.Println("=== Демонстрация 1: Нормальная работа (Closed state) ===")
	api.SetFailureRate(0.05) // 5% ошибок

	ctx := context.Background()
	for i := 0; i < 20; i++ {
		req := fmt.Sprintf("request-%d", i)

		// Создаем замыкание для выполнения запроса
		execFn := func(ctx context.Context) (interface{}, error) {
			return api.Call(ctx, req)
		}

		resp, err := cb.Execute(ctx, execFn)

		if err != nil {
			fmt.Printf("❌ Request %s failed: %v\n", req, err)
		} else {
			fmt.Printf("✅ Request %s succeeded: %v\n", req, resp)
			// Кэшируем успешный ответ
			cache.Set(req, resp)
		}

		time.Sleep(100 * time.Millisecond)
	}

	printStats(cb, api)

	// Демонстрация 2: Рост ошибок и открытие Circuit Breaker
	fmt.Println("\n=== Демонстрация 2: Рост ошибок → Open state ===")
	api.SetFailureRate(0.8) // 80% ошибок

	for i := 20; i < 40; i++ {
		req := fmt.Sprintf("request-%d", i)

		execFn := func(ctx context.Context) (interface{}, error) {
			return api.Call(ctx, req)
		}

		resp, err := cb.Execute(ctx, execFn)

		if err != nil {
			fmt.Printf("❌ Request %s failed: %v\n", req, err)
		} else {
			fmt.Printf("✅ Request %s succeeded: %v\n", req, resp)
		}

		// Показываем момент открытия Circuit Breaker
		if i == 30 {
			printStats(cb, api)
		}

		time.Sleep(100 * time.Millisecond)
	}

	printStats(cb, api)

	// Демонстрация 3: Запросы к открытому Circuit Breaker
	fmt.Println("\n=== Демонстрация 3: Запросы к открытому Circuit Breaker ===")
	fmt.Println("Запросы должны мгновенно падать с fallback ответами из кэша")

	for i := 40; i < 45; i++ {
		req := fmt.Sprintf("request-%d", i)

		start := time.Now()
		execFn := func(ctx context.Context) (interface{}, error) {
			return api.Call(ctx, req)
		}

		resp, err := cb.Execute(ctx, execFn)
		elapsed := time.Since(start)

		if err != nil {
			fmt.Printf("Request %s failed after %v: %v\n", req, elapsed, err)
		} else {
			fmt.Printf("Request %s succeeded after %v: %v (FALLBACK)\n", req, elapsed, resp)
		}
	}

	// Демонстрация 4: Восстановление в Half-Open
	fmt.Println("\n=== Демонстрация 4: Half-Open state (восстановление) ===")
	fmt.Println("Ждем открытия таймаута...")
	time.Sleep(6 * time.Second) // Ждем открытия таймаута

	api.SetFailureRate(0.1) // Снижаем ошибки до 10%

	// Отправляем серию запросов для проверки восстановления
	successCount := 0
	failCount := 0

	for i := 45; i < 60; i++ {
		req := fmt.Sprintf("request-%d", i)

		execFn := func(ctx context.Context) (interface{}, error) {
			return api.Call(ctx, req)
		}

		resp, err := cb.Execute(ctx, execFn)

		if err != nil {
			failCount++
			fmt.Printf("❌ Request %s failed: %v\n", req, err)
		} else {
			successCount++
			fmt.Printf("✅ Request %s succeeded: %v\n", req, resp)
		}

		// Показываем момент закрытия Circuit Breaker
		if cb.State() == circuitbreaker.StateClosed {
			fmt.Println("🎯 Circuit Breaker CLOSED after successful recovery!")
			break
		}

		time.Sleep(200 * time.Millisecond)
	}

	fmt.Printf("Recovery results - Success: %d, Fail: %d\n", successCount, failCount)
	printStats(cb, api)

	// Демонстрация 5: Тест конкурентности
	fmt.Println("\n=== Демонстрация 5: Конкурентные запросы ===")

	var wg sync.WaitGroup
	concurrentReqs := 10
	api.SetFailureRate(0.3) // 30% ошибок

	start := time.Now()

	for i := 0; i < concurrentReqs; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			req := fmt.Sprintf("concurrent-%d", id)
			reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			execFn := func(ctx context.Context) (interface{}, error) {
				return api.Call(ctx, req)
			}

			resp, err := cb.Execute(reqCtx, execFn)

			if err != nil {
				fmt.Printf("  Goroutine %d: ❌ %v\n", id, err)
			} else {
				fmt.Printf("  Goroutine %d: ✅ %v\n", id, resp)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("Все %d конкурентных запросов выполнены за %v\n", concurrentReqs, elapsed)
	printStats(cb, api)

	// Демонстрация 6: Graceful degradation и адаптивный порог
	fmt.Println("\n=== Демонстрация 6: Graceful degradation & Adaptive threshold ===")

	// Создаем новый Circuit Breaker для демонстрации
	cache2 := NewSimpleFallbackCache(100, 5*time.Minute)
	defer cache2.Stop()

	config2 := circuitbreaker.DefaultConfig()
	config2.FallbackCache = cache2
	config2.MetricsCollector = metrics
	config2.ErrorThreshold = 30.0

	cb2 := circuitbreaker.NewCircuitBreaker(config2)
	defer cb2.Stop()

	// Сначала наполняем кэш успешными ответами
	fmt.Println("Наполняем кэш успешными ответами...")
	api.SetFailureRate(0.0)
	for i := 0; i < 10; i++ {
		req := fmt.Sprintf("cached-%d", i)
		execFn := func(ctx context.Context) (interface{}, error) {
			return api.Call(ctx, req)
		}
		resp, _ := cb2.Execute(ctx, execFn)
		cache2.Set(req, resp)
	}

	// Теперь вызываем ошибки
	fmt.Println("Создаем ошибки для открытия Circuit Breaker...")
	api.SetFailureRate(1.0) // 100% ошибок
	for i := 10; i < 30; i++ {
		req := fmt.Sprintf("cached-%d", i)
		execFn := func(ctx context.Context) (interface{}, error) {
			return api.Call(ctx, req)
		}
		_, err := cb2.Execute(ctx, execFn)
		if err != nil {
			return
		}
	}

	// Демонстрация fallback ответов
	fmt.Println("Демонстрация fallback ответов из кэша:")
	for i := 0; i < 5; i++ {
		req := fmt.Sprintf("cached-%d", i)
		execFn := func(ctx context.Context) (interface{}, error) {
			return api.Call(ctx, req)
		}

		resp, err := cb2.Execute(ctx, execFn)

		if err != nil {
			fmt.Printf("Request %s failed: %v\n", req, err)
		} else {
			fmt.Printf("Request %s succeeded with FALLBACK: %v\n", req, resp)
		}
	}

	printStats(cb2, api)

	// Финальный отчет
	fmt.Println("\n=== ФИНАЛЬНЫЙ ОТЧЕТ ===")
	fmt.Printf("Circuit Breaker state: %s\n", cb.State())
	m := cb.GetMetrics()
	fmt.Printf("Metrics - Failures: %d, Successes: %d, Error Rate: %.2f%%, Half-Open Requests: %d\n",
		m.Failures, m.Successes, m.ErrorRate, m.HalfOpenRequests)
	fmt.Printf("API Stats: %s\n", api.GetStats())

	// Использование GenericCircuitBreaker
	fmt.Println("\n=== Бонус: Generic Circuit Breaker ===")

	// Создаем типизированный Circuit Breaker
	genericCB := circuitbreaker.NewGenericCircuitBreaker(config2,
		func(ctx context.Context, req string) (string, error) {
			return api.Call(ctx, req)
		})
	defer genericCB.Stop()

	// Используем типизированный вызов
	resp, err := genericCB.Execute(ctx, "generic-request")
	if err != nil {
		fmt.Printf("Generic request failed: %v\n", err)
	} else {
		fmt.Printf("Generic request succeeded: %v\n", resp)
	}
}

func printStats(cb *circuitbreaker.CircuitBreaker, api *MockAPIClient) {
	metrics := cb.GetMetrics()
	fmt.Printf("\n📊 Текущее состояние:\n")
	fmt.Printf("  Circuit Breaker: %s\n", metrics.State)
	fmt.Printf("  Ошибки в окне: %d\n", metrics.Failures)
	fmt.Printf("  Успехи: %d\n", metrics.Successes)
	fmt.Printf("  Процент ошибок: %.2f%%\n", metrics.ErrorRate)
	fmt.Printf("  Half-Open запросов: %d\n", metrics.HalfOpenRequests)
	fmt.Printf("  API Stats: %s\n", api.GetStats())
	fmt.Println()
}
