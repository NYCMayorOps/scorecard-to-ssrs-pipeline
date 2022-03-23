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
    zero_if_null = lambda x: 0 if pd.isna(x) else x
    count_me = sum([1 if pd.isna(x) == False else 0 for x in [one, two, three, four]])
    if count_me == 0:
        return None
    return ((zero_if_null(one) + zero_if_null(two) + zero_if_null(three) + zero_if_null(four)) / count_me )

'''
df = pd.read_csv('qe126.csv')

street_mean=[]
for index, row in df.iterrows():
    street_mean.append( mean_calc(row['street_1'], row['street_2'], row['street_3'], row['street_4']))
print(street_mean)
rated_lambda = lambda x: None if pd.isna(x) else 1
#a = list(street_mean.map(rated_lambda))
a = [rated_lambda(x) for x in street_mean]
print(a)
'''
df = pd.read_csv('bkn172_no5.csv')
none_if_na_else_1 = lambda x: None if pd.isna(x) else 1

for index, row in df.iterrows():
    street_mean =  mean_calc(row['street_1'], row['street_2'], row['street_3'], row['street_4'])
    sidewalk_mean =  mean_calc(row['street_1'], row['street_2'], row['street_3'], row['street_4'])
    acceptable_lambda = lambda x : None if pd.isna(x) else (1 if x < 1.5 else 0)
    filthy_lambda = lambda x : None if pd.isna(x) else (1 if x >=1.75 else 0)
    st_acceptable = acceptable_lambda(street_mean)
    st_filthy = filthy_lambda(street_mean)
    sw_acceptable = acceptable_lambda(sidewalk_mean)
    sw_filthy = filthy_lambda(sidewalk_mean)
    #rated_lambda = lambda x: 0 if x is None or x is np.nan else 1
    st_rated = none_if_na_else_1(street_mean)
    sw_rated = none_if_na_else_1(sidewalk_mean)

    #print(f"stop_number: {row['stop_number']}, sw_mean: {sidewalk_mean}, sw_acceptable: {sw_acceptable}, sw_rated: {sw_rated}")

print(f"mean calc 1.5 {mean_calc(1.5, 1.5, None, None)}")