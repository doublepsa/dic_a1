from mrjob.job import MRJob
from mrjob.step import MRStep

import re
import json

STOPWORDS_PATH = './stopwords.txt'


class Task1(MRJob):
    stopword_set: set[str] = set()

    def init(self):
        # load stopwords
        with open(STOPWORDS_PATH, 'r') as stopword_file:
            self.stopword_set = set()
            for line in stopword_file:
                self.stopword_set.add(line.strip().lower())

    def preprocess(self, text: str):
        # case folding
        text = text.lower()

        # tokenization
        tokenization_patter = r'[ \t\d\(\)\[\]\{\}\.\!\?\,;\:\+\=\-\_"\'`~#@&*%€$§/]+'
        token_list = re.split(tokenization_patter, text)

        # stopword removal
        token_list = set([token for token in token_list if token and token not in self.stopword_set])

        return token_list

    def mapper(self, _, line: str):
        amazon_dict = json.loads(line)
        category = amazon_dict['category']
        review_text = amazon_dict['reviewText']

        token_list = self.preprocess(review_text)

        # Count term in category
            for token in token_list:
                yield (token, category), 1
                yield (token, '*'), 1  # total term occurence
                
            # Track review count per category
            #corresponds to how many times a category appears
            yield ('REVIEW_COUNT', category), 1
        
            # Track total reviews count
            yield ('TOTAL_REVIEWS', '*'), 1

        except:
            self.increment_counter("WARN", "BadJSON", 1)

     def combiner(self, key, counts):
    #optimisation
    # sum the keys we've seen so far
         yield (key, sum(counts))

     def reducer_counter(self, key, counts):
    # sum the all the results for each key
    # send all (key,count) pairs to the same reducer.
         yield None, (key,sum(counts))
     #Since all input to this step has the same key (None), a reducer single task will get all rows    
     def reducer_chisquare(self, _, key_count):
        N = 0
        category_count = defaultdict(int)
        term_count = defaultdict(int)
        term_category_count = defaultdict(int)

        for key, count in key_count:
            term, cat = key
            if term == 'TOTAL_REVIEWS':
                N = count
            elif term == 'REVIEW_COUNT':
                category_count[cat] = count
            elif term == term and cat =='*':
                term_count[term] = count
            else:
                term_category_count[(term, cat)] = count

        # 1. calculate chi2 of all terms for each category
        chi_square_cat_term = {}
        for term, cat in term_category_count:
#how many times term appears in category
            A = term_category_count[(term, cat)]
#how many times does term appear in other categories,we get all the times the term appears in all of the categories, subtract the number of times it appears in category A
            B = term_count[term] - A
            AplusB = term_count[term]
#all the terms in the category that are not term
            C = category_count[cat] - A
            AplusC = category_count[cat]
#from all the reviews, count of all reviews not in category without term 
            D = N - (A - B - C)
            chi_square =  (N * (A * D - B * C) ** 2) / ((AplusB) * (AplusC) * (B + D) * (C + D))
            if cat not in chi_square_cat_term:
                chi_square_cat_term[cat] = {}
            chi_square_cat_term[cat][term] = chi_square
        #sort categories alphabetically
        chi_square_cat_term = OrderedDict(sorted(chi_square_cat_term.items()))

        #the top 75 most discriminative terms for the category according to the chi-square test in descending order
        for cat, terms in chi_square_cat_term.items():
           chi_square_cat_term[cat] = dict(heapq.nlargest(75, terms.items(), key=lambda k: k[1]))
           if not chi_square_cat_term[cat]:
               del chi_square_cat_term[cat]

        # output for each product category with top 75 most discriminative terms 
        for cat, terms in chi_square_cat_term.items():
            yield None, str(cat) + " " + " ".join(f"{term}:{chi_square}" for term, chi_square in terms.items())
        # output for all top 75 most discriminative terms in each category
        yield None, " ".join(sorted(list(itertools.chain.from_iterable(chi_square_cat_term.values()))))

    def steps(self):
        #Multi-step jobs as we need to aggregate counts for keys first before we calculate chi_square values
        return [
           MRStep(mapper=self.mapper,combiner=self.combiner,reducer=self.reducer_counter),
           MRStep(reducer=self.reducer_chisquare)
        ]


if __name__ == '__main__':
    Task1.run()
