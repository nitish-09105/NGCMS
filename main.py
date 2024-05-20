from flask import Flask, jsonify
import pandas as pd
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
# Loading Dataset   
df = pd.read_csv('data.csv')


# 1 Fetching the whole data
@app.route('/api/data', methods=['GET'])
def get_data():
    # Convert the DataFrame to a dictionary and jsonify the result
    data_dict = df.to_dict(orient='records')
     
    return jsonify(data_dict)

# 2 Fetching the total state and their districts
state_districts = df.groupby('State name')['District name'].apply(list).to_dict()
@app.route('/api/state_and_districts', methods=['GET'])
def state_and_districts():
    json_data = []
    state_id = 1 
    for state, districts in state_districts.items():
        state_item = {
            "id": str(state_id),
            "name": state,
            "children": [{"id": str(state_id)+'.'+str(idx + 1), "name": district, "type": "district"} for idx, district in enumerate(districts)],
            "type":"state"
        }
        json_data.append(state_item)
        state_id += 1       
    return jsonify(json_data)


# 3 Fetching the total Population by state
@app.route('/api/population/<state>', methods=['GET'])
def population_by_state(state):
    # Input validation: Check if state is valid
    valid_states = set(df['State name'])
    if state not in valid_states:
        error_response = {"error": "State not found"}
        return jsonify(error_response), 404
    # Retrieve data for the specified state from the DataFrame
    state_data = df[df['State name'] == state]
    if not state_data.empty:
        # Convert DataFrame to a dictionary of district populations
        district_populations = state_data.set_index('District name')['Population'].to_dict()
        # Construct response JSON
        response = {state: district_populations}
        # Return response with status code 200 (OK)
        return jsonify(response), 200
    else:
        # If state data is empty, return error response with status code 404 (Not Found)
        error_response = {"error": "State data not found"}
        return jsonify(error_response), 404     


# 4 Fetching the male female ratio by state
@app.route('/api/male_female_ratio_by_state/<state>', methods=['GET'])
def get_male_female_ratio_by_state(state):
    # Filter the dataset by the specified state and calculate the male-female ratio
    filtered_data = df[df['State name'] == state]
    # Calculate total males and females
    total_males = filtered_data['Male'].sum()
    total_females = filtered_data['Female'].sum()
    male_female_ratio = total_males / total_females if total_females != 0 else None
    return jsonify({'male_female_ratio': male_female_ratio})

# 5 Fetching the male female percentage by district
@app.route('/api/male_female_percentage_by_district/<district>', methods=['GET'])
def get_male_female_percentage_by_district(district):
    # Extract the list of unique district names from the DataFrame
    valid_districts = df['District name'].unique()
    # Input validation: Check if district is valid
    if district not in valid_districts:
        error_response = {"error": "District not found"}
        return jsonify(error_response), 404
    # Filter the dataset by the specified district
    filtered_data = df[df['District name'] == district]
    # Calculate total population for the district
    total_population = filtered_data['Population'].sum()
    # Calculate total males and females for the district
    total_males = filtered_data['Male'].sum()
    total_females = filtered_data['Female'].sum()
    # Calculate percentages of males and females
    male_percentage = (total_males / total_population) * 100
    female_percentage = (total_females / total_population) * 100
    # Construct response JSON
    response = {
        "district": district,
        "male_percentage": male_percentage,
        "female_percentage": female_percentage
    }
    # Return response with status code 200 (OK)
    return jsonify(response), 200


# 6 Fetching the population of various religious groups by state
@app.route('/api/population/religious-groups_by_state/<state>', methods=['GET'])
def religious_groups_by_state(state):
    # Filter the dataset by the specified state
    state_data = df[df['State name'] == state]
    # Check if state data is found
    if not state_data.empty:
        # Calculate total population of the state
        total_population = state_data['Population'].sum()
        # Ensure columns used in calculations are numeric
        numeric_columns = ['Hindus', 'Muslims', 'Christians', 'Sikhs', 'Buddhists', 'Jains']
        state_data[numeric_columns] = state_data[numeric_columns].apply(pd.to_numeric, errors='coerce')
        # Calculate percentages of each religious group
        religious_percentages = {}
        for column in numeric_columns:
            percentage = (state_data[column].sum() / total_population) * 100
            religious_percentages[column] = percentage
        # Construct response JSON
        response = {
            state:{
            "religious_groups": religious_percentages
        }
        }
        # Return response with status code 200 (OK)
        return jsonify(response), 200
    else:
        # Return error response with status code 404 (Not Found)
        error_response = {
            "error": "State not found"
        }
        return jsonify(error_response), 404
    

# 7 Fetching the population of various religious groups by district
@app.route('/api/population/religious-groups_by_district/<district>', methods=['GET'])
def religious_groups_by_district(district):
    # Filter the dataset by the specified district
    district_data = df[df['District name'] == district]
    # Check if district data is found
    if not district_data.empty:
        # Calculate total population of the district
        total_population = district_data['Population'].sum()
        # Calculate percentages of each religious group
        hindus_percentage = (district_data['Hindus'].sum() / total_population) * 100
        muslims_percentage = (district_data['Muslims'].sum() / total_population) * 100
        christians_percentage = (district_data['Christians'].sum() / total_population) * 100
        sikhs_percentage = (district_data['Sikhs'].sum() / total_population) * 100
        buddhists_percentage = (district_data['Buddhists'].sum() / total_population) * 100
        jains_percentage = (district_data['Jains'].sum() / total_population) * 100
        # Construct response JSON
        response = {
            district: {
                "Hindus": hindus_percentage,
                "Muslims": muslims_percentage,
                "Christians": christians_percentage,
                "Sikhs": sikhs_percentage,
                "Buddhists": buddhists_percentage,
                "Jains": jains_percentage
            }
        }
        # Return response with status code 200 (OK)
        return jsonify(response), 200
    else:
        # Return error response with status code 404 (Not Found)
        error_response = {
            "error": "District not found"
        }
        return jsonify(error_response), 404
    

# 8 Fetching the household ownership ratio by state
@app.route('/api/household_ownership_ratio_by_state/<state>', methods=['GET'])
def get_household_ownership_ratio_by_state(state):
    # Filter the dataset by the specified state and calculate the owned and rented household ratios
    filtered_data = df[df['State name'] == state]
    # Calculate owned household
    owned_households = filtered_data['Ownership_Owned_Households'].sum()
    # Calculate rented household
    rented_households = filtered_data['Ownership_Rented_Households'].sum()
    # Calculate the owenership ratio
    ownership_ratio = owned_households / rented_households if rented_households != 0 else None
    return jsonify({'ownership_ratio': ownership_ratio})


# 9 Fetching the household ownership percentage by district
@app.route('/api/household_ownership_percentage_by_district/<district>', methods=['GET'])
def get_household_ownership_percentage_by_district(district):
    # Filter the dataset by the specified district
    filtered_data = df[df['District name'] == district]
    
    # Check if district data is found
    if not filtered_data.empty:
        # Calculate total owned households
        owned_households = filtered_data['Ownership_Owned_Households'].sum()
        # Calculate total rented households
        rented_households = filtered_data['Ownership_Rented_Households'].sum()
        # Calculate total households
        total_households = owned_households + rented_households
        # Calculate ownership and rental percentages
        ownership_percentage = (owned_households / total_households) * 100 if total_households != 0 else 0
        rental_percentage = (rented_households / total_households) * 100 if total_households != 0 else 0
        # Construct response JSON
        response = {
            "district": district,
            "ownership_percentage": ownership_percentage,
            "rental_percentage": rental_percentage
        }
        # Return response with status code 200 (OK)
        return jsonify(response), 200
    else:
        # Return error response with status code 404 (Not Found)
        error_response = {
            "error": "District not found"
        }
        return jsonify(error_response), 404


# 10 Distribution of Education Levels in state
@app.route('/api/education_distribution_by_state/<state>', methods=['GET'])
def get_education_distribution(state):
    # Filter the dataset by the specified state
    filtered_data = df[df['State name'] == state]
    if not filtered_data.empty:
        # Calculate total Primary_Education
        primary_education = filtered_data['Primary_Education'].sum()
        # Calculate total Secondary_Education
        secondary_education = filtered_data['Secondary_Education'].sum()
        # Calculate total Higher_Education
        higher_education = filtered_data['Higher_Education'].sum()
        # Calculate total Graduate_Education
        graduate_education = filtered_data['Graduate_Education'].sum()
        # Calculate total Education
        total_education = secondary_education + higher_education+graduate_education + primary_education
        # calculate education percentages
        primary_Education_percentage = (primary_education / total_education) * 100 if total_education != 0 else 0
        Secondary_Education_percentage = (secondary_education / total_education) * 100 if total_education != 0 else 0
        Higher_education_percentage = (higher_education / total_education) * 100 if total_education != 0 else 0
        Graduate_education_percentage = (graduate_education / total_education) * 100 if total_education != 0 else 0
        # Construct response JSON
        response={
            state:{
            "primary_Education_percentage": primary_Education_percentage,
            "Secondary_Education_percentage": Secondary_Education_percentage,
            "Higher_education_percentage": Higher_education_percentage,
            "Graduate_education_percentage": Graduate_education_percentage
        }
        }
        # Return response with status code 200 (OK)
        return jsonify(response), 200
    else:
        # Return error response with status code 404 (Not Found)
        error_response = {
            "error": "State not found"
        }
        return jsonify(error_response), 404
    
    
# 11 Distribution of Education Levels in district    
@app.route('/api/education_distribution_by_district/<district>', methods=['GET'])
def get_education_distribution_by_district(district):
    # Filter the dataset by the specified district
    filtered_data = df[df['District name'] == district]
    if not filtered_data.empty:
        # Calculate total Primary_Education
        primary_education = filtered_data['Primary_Education'].sum()
        # Calculate total Secondary_Education
        secondary_education = filtered_data['Secondary_Education'].sum()
        # Calculate total Higher_Education
        higher_education = filtered_data['Higher_Education'].sum()
        # Calculate total Graduate_Education
        graduate_education = filtered_data['Graduate_Education'].sum()
        # Calculate total Education
        total_education = secondary_education + higher_education + graduate_education + primary_education
        # calculate education percentages
        primary_education_percentage = (primary_education / total_education) * 100 if total_education != 0 else 0
        secondary_education_percentage = (secondary_education / total_education) * 100 if total_education != 0 else 0
        higher_education_percentage = (higher_education / total_education) * 100 if total_education != 0 else 0
        graduate_education_percentage = (graduate_education / total_education) * 100 if total_education != 0 else 0
        # Construct response JSON
        response = {
            district: {
                "primary_education_percentage": primary_education_percentage,
                "secondary_education_percentage": secondary_education_percentage,
                "higher_education_percentage": higher_education_percentage,
                "graduate_education_percentage": graduate_education_percentage
            }
        }
        # Return response with status code 200 (OK)
        return jsonify(response), 200
    else:
        # Return error response with status code 404 (Not Found)
        error_response = {
            "error": "district not found"
        }
        return jsonify(error_response), 404

# Fetching the total gender Population by state
@app.route('/api/population/<state>/gender', methods=['GET'])
def gender_by_state(state):
     
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





    


# Fetching the literacy percent by age group  by state
@app.route('/api/literate_percentage_by_age_group/<age_group>', methods=['GET'])
def get_literate_percentage_by_age_group(age_group):
     
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
     
    # Filter the dataset by the specified district and calculate the percentage of religious minorities
    filtered_data = df[df['District name'] == district]
    # Calculate total population
    total_population = filtered_data['Population'].sum()
    # Calculate the majority religion population
    majority_religion_population = filtered_data[['Hindus', 'Muslims', 'Christians']].sum().max()
    # Calculate the religious minority percentage
    religious_minority_percentage = ((total_population - majority_religion_population) / total_population) * 100 if total_population != 0 else None
    return jsonify({'religious_minority_percentage': religious_minority_percentage})







# Fetching the power parity percentage by district
@app.route('/api/power_parity_percentage_by_district/<district>', methods=['GET'])
def get_power_parity_percentage_by_district(district):
     
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


