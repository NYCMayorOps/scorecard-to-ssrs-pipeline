#print(len([1, 2, None, 4]))
import numpy as np
import pandas as pd
#print(sum([1, 2, None]))
#print(1 * None)
#print( 1 / None)
#print(None / None)
mylist = [1, 2, 3, None]
#mylist = [1 if x is not None else 0 for x in mylist]
#print(sum(mylist))
street_mean = 1.4
#print(map(lambda x: 1 if x < 1.5 else 0, street_mean ))
street_mean = 1.6
#print(map(lambda x: 1 if x < 1.5 else 0, street_mean ))

#[1.0, 2.0, 3.0].apply(lambda x : int(x))
#foo = map(lambda x: int(x), [1.0, 2.0, 3.0])
#print(foo)


def mean_calc(one, two, three, four):
    denominator = None
    if (one or two or three or four) == False: 
        return None
    else: 
        #cannot sum none types
        count_me = sum([1 if x is not None else 0 for x in [one, two, three, four]])
    return ((one + two + three + four) / count_me )

df = pd.read_csv('qe126.csv')

street_mean=[]
for index, row in df.iterrows():
    street_mean.append( mean_calc(row['street_1'], row['street_2'], row['street_3'], row['street_4']))
print(street_mean)
rated_lambda = lambda x: None if pd.isna(x) else 1
#a = list(street_mean.map(rated_lambda))
a = [rated_lambda(x) for x in street_mean]
print(a)