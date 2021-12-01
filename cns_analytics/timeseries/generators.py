import numpy as np


class TimeSeriesBaseGenerator:
    def __init__(self):
        self.batch_size = 1024
        self.data = []
        self._len_data = 0
        self.idx = 0

    def generate_more(self):
        raise NotImplementedError

    def __iter__(self):
        return self

    def __next__(self):
        if self.idx >= self._len_data:
            self.generate_more()
            self._len_data += self.batch_size

        value = self.data[self.idx]
        self.idx += 1

        return value

    def __getitem__(self, item):
        if item >= self._len_data:
            self.generate_more()
            self._len_data += self.batch_size

        return self.data[item]


class NormalDistributedPriceGenerator(TimeSeriesBaseGenerator):
    def generate_more(self):
        new_data = np.cumsum((np.random.random(self.batch_size) >= 0.5) * 2 - 1)

        try:
            last_value = self.data[-1]
        except IndexError:
            last_value = 0
            new_data = new_data[:self.batch_size - 1]
            new_data = np.insert(new_data, 0, 0)

        self.data.extend((new_data + last_value).tolist())
