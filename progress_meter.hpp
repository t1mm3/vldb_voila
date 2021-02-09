#ifndef H_PROGRESS_METER
#define H_PROGRESS_METER

#include <ctime>
#include <string.h>
#include <stdio.h>

struct ProgressMeter {
	template<size_t N, typename T>
	struct Window {
		static constexpr size_t WINDOW_SIZE = N;
		T m_data[WINDOW_SIZE];
		size_t m_write_pos;

		T m_sum;
		
		T sum() const {
			return m_sum;
		}


		size_t num_collected() const {
			return m_write_pos;
		}

		bool empty() const {
			return !m_write_pos;
		}

		bool full() const {
			return m_write_pos >= WINDOW_SIZE;
		}

		size_t size() const {
			return full() ? WINDOW_SIZE : m_write_pos;
		}

		double avg() const {
			if (empty()) {
				return 0.0;
			}
			return (double)sum() / (double)size();
		}


		void add(const T& v) {
			size_t index = m_write_pos % WINDOW_SIZE;
			m_sum -= m_data[index];
			m_data[index] = v;
			m_sum += m_data[index];

			m_write_pos++;
		}

		void clear() {
			for (size_t i=0; i<WINDOW_SIZE; i++) {
				m_data[i] = 0;
			}
			m_write_pos = 0;
			m_sum = 0;
		}

		Window() {
			clear();
		}

	};


private:
	size_t next_refresh = 0;
	size_t num_current = 0;
	const size_t num_total;

	clock_t last_clock;
	bool has_last = false;
	size_t last_tick = 0;
	size_t warmup_counter = 0;

	Window<4, double> speed_window;
	double report_cum_time = 0.0;

	//! Report every x secs
	const double report_sec = 2.0;

	//! Refresh every x secs
	const double refresh_sec = report_sec / 8.0;

	const size_t warmup_iterations = 10;
	const size_t min_granularity = 1;

	const double max_divergence = 2.0;


	// internal refresh ... can count and print. However doesn't influence progress
	void __refresh() {
		clock_t curr_clock = clock();

		if (has_last) {
			const bool warmup_mode = 
				warmup_counter < warmup_iterations;
			const double diff_sec = double(curr_clock - last_clock)
				/ CLOCKS_PER_SEC;
			const double speed = diff_sec / (num_current - last_tick);

			speed_window.add(speed);
			warmup_counter++;

			const size_t todo = num_total - num_current;
			const double avg = speed_window.avg();

			if (!warmup_mode && speed_window.full() &&
					(speed/max_divergence > avg || avg > max_divergence*speed)) {
				speed_window.clear();

#if 1
				bool lo = speed/max_divergence < avg;
				bool hi = avg < max_divergence*speed;

				printf("Too far off, clear window.\n"
					"Avg %f, Speed %f, between %f, %f, lo %d, hi %d\n",
					avg, speed, speed/max_divergence,
					max_divergence*speed, lo, hi);
#endif
				warmup_counter = 0;
			}

			report_cum_time += diff_sec;

			if (report_cum_time >= report_sec) {
				output((double)num_current / (double)num_total,
					(double)todo * avg);

				report_cum_time = 0.0;
			}

			next_refresh = num_current +
				(warmup_mode ? min_granularity : (refresh_sec / avg));
		} else {
			has_last = true;
			next_refresh = num_current + min_granularity;
		}


		last_tick = num_current;
		last_clock = clock();
	}


public:
	// Update progress to the next tick
	void operator()() {
		num_current++;

		if (num_current >= next_refresh) {
			__refresh();
		}
	}

	virtual void output(double progress, double sec_to_finish) {
		std::cerr << (int)(progress*100.0) << "% done ... "
			<< (int)sec_to_finish << " secs to go\n";
	}

	ProgressMeter(size_t total) : num_total(total) {
	}
};

#endif