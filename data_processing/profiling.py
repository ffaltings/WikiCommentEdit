from time import perf_counter

class Profiled:
    """Helper class to add static profiling of functions and generators"""
    perf_stats = {}

    @classmethod
    def generator(cls, gen_func):
        """Decorate an instance generator with this to add profiling output"""
        cls.perf_stats[gen_func.__name__] = stats = {"elapsed": 0.0, "count_in": 0}
        def wrapped_gen(instance):
            stats["count_in"] += 1
            start_time = perf_counter()
            result = yield from gen_func(instance)
            stats['elapsed'] += perf_counter() - start_time
            return result
        return wrapped_gen       

