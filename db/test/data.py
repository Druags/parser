import pandas as pd

good_user_data = pd.DataFrame({'url': [1], 'sex': [1]})
good_user_full_data = pd.DataFrame({'url': [1],
                                    'sex': [1],
                                    'favorite_titles': [{'test_title_url_1', 'test_title_url_2'}],
                                    'abandoned_titles': [{'test_title_url_3'}],
                                    'favorite_tags': [
                                        {'Романтика': '726', 'Драма': '548', 'Яой': '525', 'Фэнтези': '301',
                                         'Комедия': '286', 'Повседневность': '226'}]})

good_title_tag_data = pd.DataFrame({'url': ['test'], 'tags': [{'test_tag_3'}]})
good_title_data = pd.DataFrame({'url': ['test-title-url']})
good_author_data = pd.DataFrame({'name': ['Test_Author']})
good_publisher_data = pd.DataFrame({'name': ['test_publisher_name']})
good_artist_data = pd.DataFrame({'name': ['test_artist_name']})
good_tag_data = pd.DataFrame({'name': ['test_tag_name']})
good_abandoned_title = pd.DataFrame({'user_id': [1], 'title_url': ['test-title-url']})
good_selected_title_data = pd.DataFrame({'title_id': [1], 'url': ['test-title-url']})

good_title_full_data = pd.DataFrame({'type': ['Манхва', 'Манга'],
                                     'name': ['Test', 'Test_2'],
                                     'url': ['test', 'test_2'],
                                     'tags': [{"test_tag_1", "test_tag_2"}, set()],
                                     'stats': [
                                         {'Читаю': 451, 'В планах': 1073, 'Брошено': 290, 'Прочитано': 358,
                                          'Любимое': 28, 'Другое': 154},
                                         ""],
                                     'ratings': [{10: 21, 9: 15, 8: 1, 7: 21, 6: 9, 5: 21}, dict()],
                                     'release_year': [2015, 2024],
                                     'publication_status': ['Завершён', 'Завершён'],
                                     'translation_status': ['Завершен', 'Завершен'],
                                     'authors': [{'Test_Author'}, {'Test_Author', 'Test_Author_2'}],
                                     'artists': [{'Test_Artist'}, {'Test_Author', 'Test_Author_2'}],
                                     'publishers': [{'Test_Publisher'}, {'Test_Publisher', 'Test_Publisher_2'}],
                                     'chapters_uploaded': [1, 19],
                                     'age_rating': ['Unknown', '18+'],
                                     'release_formats': [{'Test_format'}, {'Test_format', 'Test_format_2'}]
                                     })
