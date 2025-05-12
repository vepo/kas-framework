package dev.vepo.kafka.maestro.adapter.stats;

import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class StatsValuesTest {

    @Nested
    class LinearRegression {
        @Test
        void growingTest() {
            var values = new StatsValues(10, 50);
            values.add(2, 1);
            assertThat(values.hasData()).isFalse();
            range(2, 10).forEach(i -> values.add(i * 2l, i));
            assertThat(values.hasData()).isFalse();
            values.add(20, 10);
            assertThat(values.hasData()).isTrue();
            assertThat(values.regression().slope()).isEqualTo(2.0, Assertions.withPrecision(0.001));
            assertThat(values.regression().root()).isEqualTo(0.0, Assertions.withPrecision(0.001));
        }

        @Test
        void constantTest() {
            var values = new StatsValues(10, 50);
            values.add(20, 1);
            assertThat(values.hasData()).isFalse();
            range(2, 10).forEach(i -> values.add(20, i));
            assertThat(values.hasData()).isFalse();
            values.add(20, 10);
            assertThat(values.hasData()).isTrue();
            assertThat(values.regression().slope()).isZero();
            assertThat(values.regression().root()).isInfinite();
        }

        @Test
        void irregularTest() {
            var values = new StatsValues(10, 50);
            values.add(20, 1);
            assertThat(values.hasData()).isFalse();
            range(2, 10).forEach(i -> values.add(i % 2 == 0 ? 10 : 20, i));
            assertThat(values.hasData()).isFalse();
            values.add(10, 10);
            values.add(20, 11);
            assertThat(values.hasData()).isTrue();
            assertThat(values.regression().slope()).isZero();
            assertThat(values.regression().root()).isInfinite();
        }

        @Test
        void memoryTest() {
            var values = new StatsValues(10, 50);
            range(0, 50).forEach(i -> values.add(50, i + 1));
            assertThat(values.hasData()).isTrue();
            assertThat(values.regression().slope()).isZero();
            assertThat(values.regression().root()).isInfinite();

            range(50, 60).forEach(i -> values.add(i, i + 1));
            assertThat(values.hasData()).isTrue();
            assertThat(values.regression().slope()).isPositive();
            assertThat(values.regression().root()).isFinite();

            range(60, 100).forEach(i -> values.add(i, i + 1));
            assertThat(values.hasData()).isTrue();
            assertThat(values.regression().slope()).isOne();
            assertThat(values.regression().root()).isOne();
        }

        @Test
        void decliningTest() {
            var values = new StatsValues(10, 50);
            values.add(20, 1);
            assertThat(values.hasData()).isFalse();
            range(1, 9).forEach(i -> values.add(20 - (i * 2l), i + 1));
            assertThat(values.hasData()).isFalse();
            values.add(2, 10);
            assertThat(values.hasData()).isTrue();
            assertThat(values.regression().slope()).isEqualTo(-2.0, Assertions.withPrecision(0.001));
            assertThat(values.regression().root()).isEqualTo(11.0, Assertions.withPrecision(0.001));
        }
    }
}
