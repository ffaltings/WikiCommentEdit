import json
from time import perf_counter
from collections import OrderedDict

class Profiled:
    """Helper class to add static profiling of functions and generators"""
    perf_stats = OrderedDict()

    total_count = 0

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
        wrapped_gen.__name__ = gen_func.__name__
        return wrapped_gen

    @classmethod
    def summarize(cls, processor_functions):
        step_names = [p.__name__ for p in processor_functions if p.__name__ in cls.perf_stats]
        stats = [cls.perf_stats[name] for name in step_names]
        counts = [s["count_in"] for s in stats]

        summary = []
        summary.append(json.dumps(cls.perf_stats))
        summary.append("Total input count = {}. Total output count = {}. Yield* = {}%".format(
            counts[0], cls.total_count, cls.total_count * 100 / counts[0]))
        for (step, (in_count, out_count)) in zip(step_names, zip(counts, counts[1:] + [cls.total_count])):
            accepted = (out_count * 100) / in_count
            rejected = 100 - accepted
            line = "{}: in: {} -> out: {} (accepted {:.1f}%, rejected {:.1f})".format(step, in_count, out_count, accepted, rejected)
            summary.append(line)

        return "\n".join(summary)


        




