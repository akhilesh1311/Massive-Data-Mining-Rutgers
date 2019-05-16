from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np

class Question1(MRJob):
    JOBCONF = {
        'mapred.output.key.comparator.class':
            'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
        'mapred.text.key.comparator.options': '-n',
    }

    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
                   reducer=self.another_reducer),
            MRStep(reducer=self.reducer1),
            MRStep(reducer=self.reducer2)
        ]
    def mapper1(self, _, line):
        id, friends = line.split("\t")
        if friends == '':
            return
        friends = friends.split(",")
        friends.append(int(id))
        friends = [int(i) for i in friends]

        for i in range(len(friends)):
            for j in range(len(friends)):
                if int(friends[j]) == int(friends[i]):
                    continue
                if int(friends[i]) == int(id):
                    yield((int(friends[i]), int(friends[j])), 0)
                elif int(friends[j]) == int(id):
                    yield((int(friends[i]), int(friends[j])), 0)
                yield((int(friends[i]), int(friends[j])), 1)

    def another_reducer(self, word, counts):
        counts2 = np.array(list(counts))
        if (np.prod(counts2) == 0):
            pass
        else:
            yield(word, int(np.sum(counts2)))

    def reducer1(self, word, counts):
        yield(word[0], (word[1], sum(counts)))

    def reducer2(self, link, count):
        counts = list(count)
        counts_sorted = sorted(counts, key=lambda x:x[0], reverse=False)
        counts_sorted = sorted(counts_sorted, key=lambda x:x[1], reverse=True)
        yield(link, [x[0] for x in counts_sorted[:10]])

if __name__ == '__main__':
    Question1.run()
