from flask import Flask, jsonify,response
import pandas as pd
import requests
app = Flask(__name__)

# Loading Dataset
df = pd.read_csv('data.csv')


# Fetching the whole data
@app.route('/api/data', methods=['GET'])
def get_data():
    # Convert the DataFrame to a dictionary and jsonify the result
    data_dict = df.to_dict(orient='records')
    response.headers.add('Access-Control-Allow-Origin', '*')
    return jsonify(data_dict)



# Fetching the total state and their districts
state_districts = df.groupby('State name')['District name'].apply(list).to_dict()
@app.route('/api/state_and_districts', methods=['GET'])
def state_and_districts():
    json_data = []
    state_id = 1 
    for state, districts in state_districts.items():
        state_item = {
            "id": str(state_id),
            "name": state,
            "children": [{"id": str(state_id) + '.' + str(idx + 1), "name": district} for idx, district in enumerate(districts)]
        }
        json_data.append(state_item)
        state_id += 1  
    response.headers.add('Access-Control-Allow-Origin', '*')
    return jsonify(json_data)



# Fetching the total Population by state
@app.route('/api/population/<state>', methods=['GET'])
def population_by_state(state):
    response.headers.add('Access-Control-Allow-Origin', '*')
    state_data = df[df['State name'] == state]
    if not state_data.empty:
        district_populations = state_data.set_index('District name')['Population'].to_dict()
        response = {
            state: district_populations
        }
        return jsonify(response), 200
    else:
        error_response = {
            "error": "State not found"
        }
        
        return jsonify(error_response), 404
    


# Fetching the total gender Population by state
@app.route('/api/population/<state>/gender', methods=['GET'])
def gender_by_state(state):
    response.headers.add('Access-Control-Allow-Origin', '*')
    state_data = df[df['State name'] == state]
    if not state_data.empty:
        response = {state: {}}
        for index, row in state_data.iterrows():
            district = row['District name']
            male_population = row['Male']
            female_population = row['Female']
            response[state][district] = {
                "Male": male_population,
                "Female": female_population
            }
        return jsonify(response), 200
    else:
        error_response = {
            "error": "State not found"
        }
        return jsonify(error_response), 404



# Fetching the total gender Population by state
@app.route('/api/population/<state>/workers', methods=['GET'])
def workers_by_state(state):
    response.headers.add('Access-Control-Allow-Origin', '*')
    state_data = df[df['State name'] == state]
    if not state_data.empty:
        response = {state: {}}
        for index, row in state_data.iterrows():
            district = row['District name']
            total_workers = row['Workers']
            male_workers = row['Male_Workers']
            female_workers = row['Female_Workers']
            response[state][district] = {
                "totalworkers": total_workers,
                "male_workers": male_workers,
                "female_workers": female_workers
            }
        return jsonify(response), 200
    else:
        error_response = {
            "error": "State not found"
        }
        return jsonify(error_response), 404



# Fetching the total literate workers by state
@app.route('/api/population/<state>/literate', methods=['GET'])
def literate_workers_by_state(state):
    response.headers.add('Access-Control-Allow-Origin', '*')
    state_data = df[df['State name'] == state]
    if not state_data.empty:
        response = {state: {}}
        for index, row in state_data.iterrows():
            district = row['District name']
            total_literate = row['Literate']
            male_literate = row['Male_Literate']
            female_literate = row['Female_Literate']
            response[state][district] = {
                "total_literate": total_literate,
                "male_literate": male_literate,
                "female_literate": female_literate
            }
        return jsonify(response), 200
    else:
        error_response = {
            "error": "State not found"
        }
        return jsonify(error_response), 404



# Fetching the population of various religious groups by state
@app.route('/api/population/<state>/religious-groups', methods=['GET'])
def religious_groups_by_state(state):
    response.headers.add('Access-Control-Allow-Origin', '*')
    state_data = df[df['State name'] == state]
    if not state_data.empty:
        response = {state: {}}
        for index, row in state_data.iterrows():
            district = row['District name']
            hindus = row['Hindus']
            muslims = row['Muslims']
            christians = row['Christians']
            sikhs = row['Sikhs']
            buddhists = row['Buddhists']
            jains = row['Jains']
            response[state][district] = {
                "Hindus": hindus,
                "Muslims": muslims,
                "Christians": christians,
                "Sikhs": sikhs,
                "Buddhists": buddhists,
                "Jains": jains
            }
        return jsonify(response), 200
    else:
        error_response = {
            "error": "State not found"
        }
        return jsonify(error_response), 404

    


# Fetching the literacy percent by age group  by state
@app.route('/api/literate_percentage_by_age_group/<age_group>', methods=['GET'])
def get_literate_percentage_by_age_group(age_group):
    response.headers.add('Access-Control-Allow-Origin', '*')
    # Filter the dataset by the specified age group 
    filtered_data = df[df['Age_Group_' + age_group] == 1]
    # Calculate total population
    total_population = filtered_data['Population'].sum()
    # Calculate literate population
    literate_population = filtered_data['Literate'].sum()
    # Calculate literate percentage
    literate_percentage = (literate_population / total_population) * 100 if total_population != 0 else None
    return jsonify({'literate_percentage': literate_percentage})



# Fetching the religious minority percentage by district
@app.route('/api/religious_minority_percentage_by_district/<district>', methods=['GET'])
def get_religious_minority_percentage_by_district(district):
    response.headers.add('Access-Control-Allow-Origin', '*')
    # Filter the dataset by the specified district and calculate the percentage of religious minorities
    filtered_data = df[df['District name'] == district]
    # Calculate total population
    total_population = filtered_data['Population'].sum()
    # Calculate the majority religion population
    majority_religion_population = filtered_data[['Hindus', 'Muslims', 'Christians']].sum().max()
    # Calculate the religious minority percentage
    religious_minority_percentage = ((total_population - majority_religion_population) / total_population) * 100 if total_population != 0 else None
    return jsonify({'religious_minority_percentage': religious_minority_percentage})



# Fetching the household ownership ratio by state
@app.route('/api/household_ownership_ratio_by_state/<state>', methods=['GET'])
def get_household_ownership_ratio_by_state(state):
    response.headers.add('Access-Control-Allow-Origin', '*')
    # Filter the dataset by the specified state and calculate the owned and rented household ratios
    filtered_data = df[df['State name'] == state]
    # Calculate owned household
    owned_households = filtered_data['Ownership_Owned_Households'].sum()
    # Calculate rented household
    rented_households = filtered_data['Ownership_Rented_Households'].sum()
    # Calculate the owenership ratio
    ownership_ratio = owned_households / rented_households if rented_households != 0 else None
    return jsonify({'ownership_ratio': ownership_ratio})



# Fetching the power parity percentage by district
@app.route('/api/power_parity_percentage_by_district/<district>', methods=['GET'])
def get_power_parity_percentage_by_district(district):
    response.headers.add('Access-Control-Allow-Origin', '*')
    # Filter the dataset by the specified district and calculate the power parity percentage
    filtered_data = df[df['District name'] == district]
    # Calculate total power parity
    total_power_parity = filtered_data['Total_Power_Parity'].sum()
    # Calculate total population
    population = filtered_data['Population'].sum()
    # Calculate power parity percentage
    power_parity_percentage = (total_power_parity / population) * 100 if population != 0 else None
    return jsonify({'power_parity_percentage': power_parity_percentage})



if __name__ == '__main__':
    app.run(debug=True)


