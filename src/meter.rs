use time;

use syncbox::atomic::{AtomicI64, Ordering};

use ewma::EWMA;
use metric::{Metric, MetricValue};

// A MeterSnapshot
#[derive(Debug)]
pub struct MeterSnapshot {
    pub count: i64,
    pub rates: [f64; 3],
    pub mean: f64
}

/// A Meter measures the rate at which a set of events occur.
///
/// Just like the Unix load averages visible in `top`.
pub trait Meter : Metric {
    /// Returns the number of events which have been marked.
    fn count(&self) -> i64;

    /// Returns the mean rate at which events have occurred since the meter was created.
    fn mean_rate(&self) -> f64;

    /// Returns the one-minute exponentially-weighted moving average rate at which events have
    /// occurred since the meter was created.
    fn m01rate(&self) -> f64;

    /// Returns the five-minute exponentially-weighted moving average rate at which events have
    /// occurred since the meter was created.
    fn m05rate(&self) -> f64;

    /// Returns the fifteen-minute exponentially-weighted moving average rate at which events have
    /// occurred since the meter was created.
    fn m15rate(&self) -> f64;

    /// Mark the occurrence of a given number of events.
    fn mark(&self, value: i64);
}

pub trait Clock: Send + Sync {
    fn now(&self) -> i64;
}

struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> i64 {
        time::get_time().sec
    }
}

// A StdMeter struct
pub struct StdMeter<C: Clock = SystemClock> {
    clock: C,

    birthstamp: i64,
    prev: AtomicI64,

    count: AtomicI64,
    rates: [EWMA; 3],
}

impl<C: Clock> StdMeter<C> {
    fn with(clock: C) -> StdMeter<C> {
        let birthstamp = clock.now();

        StdMeter {
            count: AtomicI64::new(0),
            clock: clock,
            birthstamp: birthstamp,
            prev: AtomicI64::new(birthstamp),
            rates: [EWMA::m01rate(), EWMA::m05rate(), EWMA::m15rate()],
        }
    }

    fn tick_maybe(&self) {
        let now = self.clock.now();
        let old = self.prev.load(Ordering::SeqCst);
        let elapsed = now - old;

        if elapsed > 5 {
            // Clock values should monotonically increase, so no ABA problem here is possible.
            if self.prev.compare_and_swap(old, now - elapsed % 5, Ordering::SeqCst) == old {
                let ticks = elapsed / 5;

                for _ in 0..ticks {
                    for rate in &self.rates {
                        rate.tick();
                    }
                }
            }
        }
    }
}

impl StdMeter<SystemClock> {
    pub fn new() -> StdMeter {
        StdMeter::with(SystemClock)
    }
}

impl<C: Clock> Meter for StdMeter<C> {
    fn count(&self) -> i64 {
        self.count.load(Ordering::SeqCst)
    }

    fn mean_rate(&self) -> f64 {
        let count = self.count.load(Ordering::SeqCst);

        if count == 0 {
            return 0.0;
        }

        let elapsed = self.clock.now() - self.birthstamp;

        count as f64 / elapsed as f64
    }

    fn m01rate(&self) -> f64 {
        self.tick_maybe();
        self.rates[0].rate()
    }

    fn m05rate(&self) -> f64 {
        self.tick_maybe();
        self.rates[1].rate()
    }

    fn m15rate(&self) -> f64 {
        self.tick_maybe();
        self.rates[2].rate()
    }

    fn mark(&self, value: i64) {
        self.tick_maybe();

        self.count.fetch_add(value, Ordering::SeqCst);

        for rate in &self.rates {
            rate.update(value);
        }
    }
}

impl<C: Clock> Metric for StdMeter<C> {
    fn export_metric(&self) -> MetricValue {
        unimplemented!();
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use super::*;

    macro_rules! assert_float_eq {
        ($x:expr, $y:expr, $d:expr) => {
            if !($x - $y < $d || $y - $x < $d) { panic!(); }
        }
    }

    #[test]
    fn zero() {
        let meter = StdMeter::new();

        assert_eq!(0, meter.count());

        assert_float_eq!(0.0, meter.mean_rate(), 1e-3);

        assert_float_eq!(0.0, meter.m01rate(), 1e-3);
        assert_float_eq!(0.0, meter.m05rate(), 1e-3);
        assert_float_eq!(0.0, meter.m15rate(), 1e-3);
    }

    #[test]
    fn non_zero() {
        #[derive(Debug)]
        struct MockClock {
            counter: AtomicUsize,
        }

        impl Clock for MockClock {
            fn now(&self) -> i64 {
                match self.counter.fetch_add(1, Ordering::SeqCst) {
                    0 | 1 => 0,
                    _ => 10,
                }
            }
        }

        let meter = StdMeter::with(MockClock { counter: AtomicUsize::new(0) });

        meter.mark(1);
        meter.mark(2);

        assert_eq!(3, meter.count());

        assert_float_eq!(0.3, meter.mean_rate(), 1e-3);

        assert_float_eq!(0.1840, meter.m01rate(), 1e-3);
        assert_float_eq!(0.1966, meter.m05rate(), 1e-3);
        assert_float_eq!(0.1988, meter.m15rate(), 1e-3);

        assert_eq!(7, meter.clock.counter.load(Ordering::SeqCst));
    }

    // #[test]
    // fn snapshot() {
    //     let m: StdMeter = StdMeter::new();
    //     m.mark(1);
    //     m.mark(1);
    //
    //     let s = m.snapshot();
    //
    //     m.mark(1);
    //
    //     assert_eq!(s.count, 2);
    //     assert_eq!(m.snapshot().count, 3);
    // }
    //
    // // Test that decay works correctly
    // #[test]
    // fn decay() {
    //     let mut m: StdMeter = StdMeter::new();
    //
    //     m.tick();
    // }
}
