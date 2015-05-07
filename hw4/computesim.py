import numpy as np

from mrjob.job import MRJob
from itertools import combinations, permutations
from operator import itemgetter
from math import isnan

from scipy.stats.stats import pearsonr


class RestaurantSimilarities(MRJob):

    def steps(self):
        "the steps in the map-reduce process"
        thesteps = [
            self.mr(mapper=self.line_mapper, reducer=self.users_items_collector),
            self.mr(mapper=self.pair_items_mapper, reducer=self.calc_sim_collector)
        ]
        return thesteps

    def line_mapper(self,_,line):
        "this is the complete implementation"
        user_id,business_id,stars,business_avg,user_avg=line.split(',')
        yield user_id, (business_id,stars,business_avg,user_avg)


    def users_items_collector(self, user_id, values):
        """
        #iterate over the list of tuples yielded in the previous mapper
        #and append them to an array of rating information
        """
        ratings = []
        for value in values:
            ratings.append(value)
        yield user_id, ratings

    def pair_items_mapper(self, user_id, values):
        """
        ignoring the user_id key, take all combinations of business pairs
        and yield as key the pair id, and as value the pair rating information
        """
        # dict = {}
        # for value in values:
        #     business_id, stars, business_avg, user_avg = value
        #     dict[business_id] = {'stars': stars, 'business_avg': business_avg, 'user_avg': user_avg}
        # for each in combinations(dict, 2):
        #     yield user_id, each

        values = sorted(values, key=itemgetter(0))
        for i in range(len(values)):
            for j in range(len(values)):
                if i < j:
                    business_id1, stars1, business_avg1, user_avg1 = values[i]
                    business_id2, stars2, business_avg2, user_avg2 = values[j]
                    if business_id1 < business_id2:
                        yield (business_id1, business_id2), [(stars1, business_avg1, user_avg1), (stars2, business_avg2, user_avg2)]
                    else: 
                        yield (business_id2, business_id1), [(stars2, business_avg2, user_avg2), (stars1, business_avg1, user_avg1)]


    def calc_sim_collector(self, key, values):
        """
        Pick up the information from the previous yield as shown. Compute
        the pearson correlation and yield the final information as in the
        last line here.
        """
        (rest1, rest2), common_ratings = key, values

        diff1, diff2 = [], []
        rho = 0
        
        for each in common_ratings:
            stars1, business_avg1, user_avg1 = each[0]
            stars2, business_avg2, user_avg2 = each[1]

            diff1.append(float(stars1) - float(user_avg1))
            diff2.append(float(stars2) - float(user_avg2))
            
        rho = pearsonr(diff1, diff2)[0]
        if isnan(rho):
            rho = 0

        yield (rest1, rest2), (rho, len(list(common_ratings)))

#Below MUST be there for things to work
if __name__ == '__main__':
    RestaurantSimilarities.run()