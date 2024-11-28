import json

# Load the JSON file
with open('Conflict CoCs.json', 'r') as file:
    data = json.load(file)

# Access the list inside 'categoryOptionCombos'
combos = data['categoryOptionCombos']

# Use a set to track unique IDs and filter duplicates
unique_ids = set()
unique_combos = []

for combo in combos:
    if combo['id'] not in unique_ids:
        unique_ids.add(combo['id'])
        unique_combos.append(combo)

# Update the data structure with unique entries
data['categoryOptionCombos'] = unique_combos

# Save the cleaned JSON back to a file
with open('../../BeforeRepoCreated/cleaned_file_Conflict CoCs.json', 'w') as file:
    json.dump(data, file, indent=4)

# Print result to verify
print(json.dumps(data, indent=4))
