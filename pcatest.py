import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

# Example data
data = [{
  "_Country_Name_": "Australia",
  "Country_Code": "193",
  "Indicator_Name": "Labor Markets, Employment, Index",
  "Indicator_Code": "A",
  "Time_Period": "2022",
  "Value": "123.44444",
  "Status": ""
}, {
  "_Country_Name_": "Austria",
  "Country_Code": "122",
  "Indicator_Name": "Labor Markets, Employment, Index",
  "Indicator_Code": "B",
  "Time_Period": "2022",
  "Value": "107.312132045317",
  "Status": ""
}, {
  "_Country_Name_": "Bel",
  "Country_Code": "10",
  "Indicator_Name": "Labor Markets, Employment, Index",
  "Indicator_Code": "C",
  "Time_Period": "2022",
  "Value": "10731213204531.7",
  "Status": ""
}, {
  "_Country_Name_": "Ko",
  "Country_Code": "111",
  "Indicator_Name": "Labor Markets, Employment, Index",
  "Indicator_Code": "D",
  "Time_Period": "2022",
  "Value": "102132045317",
  "Status": ""
}, {
  "_Country_Name_": "Ir",
  "Country_Code": "333",
  "Indicator_Name": "Labor Markets, Employment, Index",
  "Indicator_Code": "E",
  "Time_Period": "2022",
  "Value": "3121317",
  "Status": ""
}, {
  "_Country_Name_": "US",
  "Country_Code": "444",
  "Indicator_Name": "Labor Markets, Employment, Index",
  "Indicator_Code": "F",
  "Time_Period": "2022",
  "Value": "3531.7",
  "Status": ""
}] 

values = np.array([float(entry['Value']) for entry in data])
indicator_codes = np.array([entry['Indicator_Code'] for entry in data])

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

top_500_indicator_indices = np.argsort(sum_absolute_loadings)[-3:]

top_500_indicators = indicator_codes_encoded.columns[top_500_indicator_indices]

print("Top 500 indicators:")
for indicator in top_500_indicators:
    print(indicator)
