from collections import Counter, defaultdict
from typing import Optional, List


class MarkovChain:
    """ Finds probabilities for transitions
    Usage:
        >>> mc = MarkovChain()
        >>> for category in [1, 1, 1, 1, 2, 3, 1, 1, 1, 2, 3]:
        >>>     mc.add_observation(category)
        >>> print(mc.get_stats(depth=1))
    Will show that 2->3 100%, 3->1 100%, 1->1 80%, 1->2 20%
    """

    def __init__(self):
        self.observations = []
        self._mask: Optional[List] = None

    def add_observation(self, observation):
        self.observations.append(observation)

    def extend_observations(self, observations: List):
        self.observations.extend(observations)

    def get_entries(self, depth: int):
        entries = []

        for i in range(depth, len(self.observations) - 1):
            entries.append(tuple(self.observations[i-depth: i + 1]))

        return entries

    def set_mask(self, mask: Optional[List]):
        """Mask selects observations, but does not change their stats"""
        self._mask = mask

    def get_stats(self, depth: int, pct=False):
        stats = defaultdict(Counter)

        for i in range(depth, len(self.observations) - 1):
            if self._mask is None or self._mask[i - depth]:
                stats[tuple(self.observations[i - depth: i])][self.observations[i]] += 1

        stats = {key: dict(value) for key, value in stats.items()}

        if pct:
            for key, counter in stats.items():
                sum_entries = sum(counter.values())
                stats[key] = dict(counter)
                for obs in stats[key]:
                    stats[key][obs] /= sum_entries

        return stats
    
    def find_patterns(self, min_observations=100, min_significance=0.1):
        results = []
        stats = self.get_stats(1)
        key1, key2 = list(stats.values())[0].keys()
        for length in range(2, 10):
            stats = self.get_stats(length, pct=False)
            for pattern, result in stats.items():
                res1, res2 = result.get(key1, 0), result.get(key2, 0)
                if res1 + res2 < min_observations:
                    continue
                pct1, pct2 = res1 / (res1 + res2), res2 / (res1 + res2)
                significance = abs(pct1 - 0.5)
                if significance < min_significance:
                    continue
                results.append((res1 + res2, pattern, pct1 - 0.5))
        return results
