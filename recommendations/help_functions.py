import pandas as pd


def get_genres(genres: pd.Series):
    result = []
    for user_genres in genres:
        result.append(eval(user_genres).keys())
    return pd.Series(result)


def get_field_values(info_list, field_name):
    field_values = []
    for element in info_list:
        element = eval(element)
        field_values.append(element.get(field_name))
    return pd.Series(field_values)


def get_avg_rating(ratings):
    ratings = eval(ratings)
    if ratings:
        reviews = 0
        sum_reviews = 0
        for n_of_stars in ratings:
            reviews += ratings[n_of_stars]
            sum_reviews += ratings[n_of_stars] * n_of_stars

        return round(sum_reviews / reviews, 2)
    return 0


def get_total_reviews(ratings):
    ratings = eval(ratings)
    if ratings:
        reviews = 0
        for n_of_stars in ratings:
            reviews += ratings[n_of_stars]
        return reviews
    return 0


def set_names(names, alt_names):
    result = []
    for name, alt_name in zip(names, alt_names):
        if name == 'name':
            result.append(alt_name)
        else:
            result.append(name)
    return result


def clean_year(year):
    if '-' in year:
        return year.split('-')[0]
    elif 'г' in year:
        return year.split()[0]
    elif ',' in year:
        return year.split(',')[0]
    else:
        return year
