def get_json_diff(old_data: dict, new_data: dict) -> dict:
    diff = {}
    for key in new_data:
        if key not in old_data or old_data[key] != new_data[key]:
            diff[key] = {
                "old": old_data.get(key),
                "new": new_data[key]
            }
    return diff
