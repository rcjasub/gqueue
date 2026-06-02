# Go Concepts

## Zero Values
Every variable in Go has a default value when not initialized. Numbers default to `0`, booleans to `false`, strings to `""`, pointers to `nil`. This is why `maxTokens` was `0` when not set — and why we needed a guard clause.

```go
type Worker struct {
    maxTokens int  // defaults to 0 if not set
}
```

## Structs
A struct is a collection of fields grouped under one type. Think of it like an object in other languages, but without inheritance.

```go
type Job struct {
    Id       string
    Name     string
    Priority int
}
```

## Methods
Functions attached to a struct using a receiver. The receiver gives the function access to the struct's fields.

```go
func (w *Worker) processJob(ctx context.Context, job Job) {
    // w is the Worker this method belongs to
}
```

The `*` means the receiver is a pointer — changes to `w` affect the original, not a copy.

## Pointers
A pointer holds the memory address of a value rather than the value itself. Use `*` to declare a pointer type and `&` to get the address of a value.

```go
worker := &Worker{}   // & gives you a pointer to the Worker
```

Passing a pointer means the function works on the original value, not a copy. Important for structs you want to mutate.

## Goroutines
A goroutine is a lightweight thread managed by Go. You start one with the `go` keyword. Multiple goroutines run concurrently.

```go
go func() {
    // runs concurrently
}()
```

In gqueue, each worker runs in its own goroutine so multiple jobs can be processed at the same time.

## Channels
A way for goroutines to communicate. One goroutine sends a value, another receives it. Channels block until both sides are ready.

```go
ch := make(chan string)
go func() { ch <- "hello" }()
msg := <-ch  // blocks until the goroutine sends
```

## WaitGroup
Used to wait for a group of goroutines to finish. `Add(1)` before starting, `Done()` when finished, `Wait()` blocks until all are done.

```go
var wg sync.WaitGroup
wg.Add(1)
go func() {
    defer wg.Done()
    // do work
}()
wg.Wait()
```

## Defer
`defer` schedules a function call to run when the surrounding function returns — no matter how it returns (normal, error, panic). Used for cleanup.

```go
defer wg.Done()  // runs when the goroutine function exits
```

## For Loop (as while)
Go has no `while` keyword. Use `for` with a condition instead.

```go
for !allowed {
    time.Sleep(100 * time.Millisecond)
}
```

## Guard Clause
An early check at the top of a function that exits (or skips) immediately when a condition isn't met. Keeps the main logic unindented and readable.

```go
if w.maxTokens == 0 {
    // skip rate limiting
} else {
    for !w.queue.Allow(...) {
        time.Sleep(100 * time.Millisecond)
    }
}
```

## Error Handling
Go functions return errors as values — no exceptions. You check the error explicitly after every call.

```go
result, err := someFunc()
if err != nil {
    // handle the error
}
```

## Interfaces
An interface defines a set of method signatures. Any type that implements those methods satisfies the interface automatically — no explicit declaration needed.

```go
type ProcessFunc func(j Job) error  // function type used as a value
```

## Maps
A key-value store. Declare with `make`, access with brackets.

```go
handlers := make(map[string]ProcessFunc)
handlers["send-email"] = func(j Job) error { ... }
handler, ok := handlers["send-email"]  // ok is false if key doesn't exist
```

## The `ok` Pattern
Many Go operations return a second boolean value indicating success. Always check it.

```go
handler, ok := w.handlers[job.Name]
if !ok {
    // handler not found
}
```

## Callbacks (Functions as Arguments)
In Go, functions are values — you can pass them as arguments to other functions. A function passed in to be called later is called a **callback**.

```go
type ProcessFunc func(j Job, report func(pct int)) (string, error)
```

Here `report func(pct int)` is a callback. The worker builds it and passes it into the handler. The handler calls it whenever it wants to report progress — the handler doesn't need to know how it works internally.

This is useful when the caller needs to inject behavior into a function without the function knowing the details. The handler just calls `report(50)` and the worker takes care of writing to Redis.

## Closures
A closure is a function that captures variables from the scope it was defined in. Those variables stay accessible even after the outer function has returned.

```go
report := func(pct int) {
    w.queue.client.HSet(ctx, "job:"+job.Id, "progress", pct)
}
```

`report` closes over `job.Id` and `ctx` — it remembers them without needing them passed as arguments. This is how the worker builds a `report` function that already knows which job to update.

## Multiple Return Values
Go functions can return more than one value. The convention is to return the result first and an error last.

```go
func(j Job, report func(pct int)) (string, error)
```

The caller handles both:

```go
result, err := handler(job, report)
if err != nil {
    // something went wrong
}
// use result
```

This is how job handlers return both their output and any error that occurred.
