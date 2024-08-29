import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler


def principal_indicators(data, top_count):
    values = np.array([])
    indicator_codes = np.array([])
    
    for entry in data:
        if entry['Value']:
            entry['Value'] = float(entry['Value'])
        else:
            entry['Value'] = 0
        
        values = np.append(values, entry['Value'])
        indicator_codes = np.append(indicator_codes, entry['Indicator_Code'])

    # values = np.array([float(entry['Value']) for entry in data if entry['Value']])
    # indicator_codes = np.array([entry['Indicator_Code'] for entry in data])

    indicator_codes_encoded = pd.get_dummies(indicator_codes)

    # Combine values and indicator codes
    combined_data = np.hstack((values.reshape(-1, 1), indicator_codes_encoded))

    # Standardize the data
    scaler = StandardScaler()
    combined_data_standardized = scaler.fit_transform(combined_data)

    # Perform PCA
    pca = PCA()
    pca.fit(combined_data_standardized)

    principal_components = pca.components_

    absolute_loadings = np.abs(principal_components[:, 1:]) 

    sum_absolute_loadings = np.sum(absolute_loadings, axis=0)

    top_indicator_indices = np.argsort(sum_absolute_loadings)[-top_count:]

    top_indicators = indicator_codes_encoded.columns[top_indicator_indices]

    return top_indicators