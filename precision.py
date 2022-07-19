import numpy as np
class Precision():
#a class for rounding. It is done several different ways. This tries to capture all the ways until it is refactored.
    precision=8
    lambda_int = lambda x: int(x) if (type(x) == float or type(x) == int) and np.isnan(x) is False else np.nan
    
    @classmethod
    def my_round(self, number):
    #round percentages to the specified precision
        try:
            return round(number * 100, self.precision)
        except:
            return None

    