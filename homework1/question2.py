from mrjob.job import MRJob
from mrjob.step import MRStep
import os as os

class Question2(MRJob):
  frequent_items = []
  frequent_items_count = {}
  frequent_item_pairs = []
  confidence_pairs = {}
  confidence_triples = {}
  support_pairs = {}

  def __init__(self, *args, **kwargs):
    super(Question2, self).__init__(*args, **kwargs)

  JOBCONF = {
    'mapred.output.key.comparator.class':
      'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
    'mapred.text.key.comparator.options': '-n',
  }

  def steps(self):
    return [
      MRStep(mapper=self.mapper1,
             reducer=self.reducer1),
      MRStep(reducer=self.reducer2),
      MRStep(reducer=self.reducer3),
      MRStep(reducer=self.another_reducer),
      # MRStep(reducer=self.reducer4),
      # MRStep(reducer=self.reducer5),
      # MRStep(reducer=self.another_reducer2)
    ]

  def mapper1(self, _, line):
    line = line.split(" ")
    for item in line:
      if item == '':
        return
      yield (item, 1)

  def reducer1(self, item, count):
    totalCount = sum(count)
    local_item = item
    if totalCount >= 100:
      self.frequent_items.append(local_item)
      self.frequent_items_count[local_item] = totalCount
      yield (None, item)

  def reducer2(self, _, items):
    items = list(items)
    for i in range(len(items)):
      for j in range(len(items)):
        if i==j:
          continue
        yield (None, (items[i], items[j]))

  def reducer3(self, _, itemPair):
    itemPairList = list(itemPair)
    print(len(itemPairList))
    # self.support_pairs = {(key[0], key[1]): 0 for key in itemPairList}
    for item in itemPairList:
      self.support_pairs[(item[0], item[1])] = 0
      self.support_pairs[(item[1], item[0])] = 0
    with open("/home/akhil/Documents/rutgers/massive_data_mining/homework1/data/browsing.txt", "r") as f:
      for cnt, line in enumerate(f):
        # if cnt > 2000:
        #   break
        line = line.strip()
        line = line.split(" ")
        lineSet = set(line)
        for item in itemPairList:
          if (item[0] in lineSet) and (item[1] in lineSet):
            self.support_pairs[(item[0], item[1])] = self.support_pairs[(item[0], item[1])] + 1
    for item in itemPairList:
      if self.support_pairs[(item[0], item[1])] >= 100:
        self.frequent_item_pairs.append((item[0], item[1]))
        self.confidence_pairs[(item[0], item[1])] = \
          self.support_pairs[(item[0], item[1])]/self.frequent_items_count[item[0]]
        self.confidence_pairs[(item[1], item[0])] = \
          self.support_pairs[(item[0], item[1])]/self.frequent_items_count[item[1]]
        yield (None, (item[0], item[1]))

  def another_reducer(self, _, itemPair):
    itemPairList = list(itemPair)
    for item in itemPairList:
      yield (self.confidence_pairs[(item[0], item[1])], (item[0], item[1]))

  def reducer4(self, _, itemPair):
    items = list(itemPair)
    for i in range(len(items)):
      for j in range(len(self.frequent_items)):
        yield (None, (items[i][0], items[i][1], self.frequent_items[j]))

  def reducer5(self, _, itemTriples):
    itemTriplesList = list(itemTriples)
    support_triples = {(key[0], key[1], key[2]): 0 for key in itemTriplesList}
    with open("/home/akhil/Documents/rutgers/massive_data_mining/homework1/data/browsing.txt", "r") as f:
      for cnt, line in enumerate(f):
        # if cnt > 2000:
        #   break
        line = line.strip()
        line = line.split(" ")
        lineSet = set(line)
        for item in itemTriplesList:
          if (item[0] in lineSet) and (item[1] in lineSet) and (item[2] in lineSet):
            support_triples[(item[0], item[1], item[2])] = \
              support_triples[(item[0], item[1], item[2])] + 1
    for item in itemTriplesList:
      if support_triples[(item[0], item[1], item[2])] >= 100:
        if (item[0] == item[1]) or (item[0] == item[2]) or (item[1] == item[2]):
          continue
        self.confidence_triples[(item[0], item[1], item[2])] = \
        support_triples[(item[0], item[1], item[2])]/self.support_pairs\
        [(item[0], item[1])]

        self.confidence_triples[(item[0], item[2], item[1])] = \
        support_triples[(item[0], item[1], item[2])]/self.support_pairs\
        [(item[0], item[2])]

        self.confidence_triples[(item[2], item[1], item[0])] = \
        support_triples[(item[0], item[1], item[2])]/self.support_pairs\
        [(item[2], item[1])]

        yield(None, (item[0], item[1], item[2]))

  def another_reducer2(self, _, itemTriples):
    itemTriplesList = list(itemTriples)
    for item in itemTriplesList:
      yield (self.confidence_triples[(item[0], item[1], item[2])], \
             (item[0], item[1], item[2]))



if __name__ == '__main__':
  Question2.run()
  # with open(r"./output2/part-00000"):

