import pandas as pd
from utils.strategy import decision_strategies


def get_strategy_dataframe(countries, inds):
    countries = [list(i.values())[0] for i in countries]
    starategy_columnns = list(decision_strategies.keys())
    strategy_df = pd.DataFrame(index=countries, columns=starategy_columnns)
    
    # Populate the DataFrame with selected indicators for each strategy
    for ind in inds:
        indicator_name = ind['indicator_name']
        country_name = ind['country_name']
        strategy = next((key for key, value in decision_strategies.items() if indicator_name in value), None)
        
        if not strategy:
            continue
        
        try:
            if pd.isna(strategy_df.at[country_name, strategy]):
                strategy_df.at[country_name, strategy] = [indicator_name]
            
            else:
                strategy_df.at[country_name, strategy].append(indicator_name)
        
        except Exception as e:
            # index 0 is out of bounds for axis 0 with size 0
            pass

    result_df = strategy_df.reset_index()
    result_df["country"] = result_df["index"]
    result_df = result_df.drop(columns=["index"])
    result_df = result_df.where(pd.notna(result_df), None)
    result_df = result_df[
        [
            'country',
            starategy_columnns[0],
            starategy_columnns[1],
            starategy_columnns[2],
            starategy_columnns[3],
            starategy_columnns[4]
            ]
        ]
    
    results = result_df.values.tolist()
    
    return results
