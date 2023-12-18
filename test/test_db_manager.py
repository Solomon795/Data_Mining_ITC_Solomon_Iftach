from DatabaseManager import DatabaseManager
import json

def main():
    import Configuration
    c = Configuration.Configuration()


    m = DatabaseManager(c, 'ice nucleation')

    # sql_command = 'delete from publications_by_topics'
    # m._sql_run_execute(sql_command)
    # sql_command = 'delete from publications_by_authors'
    # m._sql_run_execute(sql_command)
    # sql_command = 'delete from topics'
    # m._sql_run_execute(sql_command)
    # sql_command = 'delete from authors'
    # m._sql_run_execute(sql_command)
    # sql_command = 'delete from publications'
    # m._sql_run_execute(sql_command)
    # sql_command = 'delete from publications_types'
    # m._sql_run_execute(sql_command)


    print("######### BEFORE INSERTION")
    sql_command = "select * from publications_types"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for ptype in result:
        print(f"pub_type:{ptype}")
    sql_command = "select * from topics"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    print(f"top:{result}")
    sql_command = "select * from publications"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for pub in result:
        print(f"pub:{pub}")
    sql_command = "select * from authors"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for aut in result:
        print(f"aut:{aut}")
    sql_command = "select * from publications_by_authors"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for pub_aut in result:
        print(f"pub_aut:{pub_aut}")
    sql_command = "select * from publications_by_topics"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for pub_top in result:
        print(f"pub_top:{pub_top}")

    print(f"topic id:{m._topic_id}")

    import os
    file_path = 'data.txt'

    if os.path.exists(file_path):
        print(f'The file {file_path} exists.')
        with open(file_path, "r") as file:
            pubs_list = []
            pubs_list = json.load(file)
            m.insert_publications_info(pubs_list)
        return
    else:
        print(f'The file {file_path} does not exist.')

    # pubs = [{"id": 1,"title": "t1", "site": "s1","publication_type": "Article",  "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 2, "title": "t2", "site": "s2", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 3, "title": "t3", "site": "s3", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 4, "title": "t4", "site": "s4", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 5, "title": "t5", "site": "s5", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 6, "title": "t6", "site": "s6", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 7, "title": "t7", "site": "s7", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 8, "title": "t8", "site": "s8", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 9, "title": "t9", "site": "s9", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 10, "title": "t10", "site": "s10", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 11, "title": "t11", "site": "s11", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 12, "title": "t12", "site": "s12", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 13, "title": "t13", "site": "s13", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 14, "title": "t14", "site": "s14", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 15, "title": "t15", "site": "s15", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 16, "title": "t16", "site": "s16", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 17, "title": "t17", "site": "s17", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 18, "title": "t18", "site": "s18", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 19, "title": "t19", "site": "s19", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100},
    #         {"id": 20, "title": "t20", "site": "s20", "publication_type": "Article", "journal": "j3",
    #          "authors": ["abba", 'saba'], "year": 2023, "reads": 99, "citations": 100}]
    #
    m.insert_publications_info(pubs)

    print("######### AFTER INSERTION")
    sql_command = "select * from topics"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    print(f"top:{result}")
    sql_command = "select * from publications"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for pub in result:
        print(f"pub:{pub}")
    sql_command = "select * from authors"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for aut in result:
        print(f"aut:{aut}")
    sql_command = "select * from publications_by_authors"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for pub_aut in result:
        print(f"pub_aut:{pub_aut}")
    sql_command = "select * from publications_by_topics"
    result = m._sql_run_fetch_command(sql_command, fetch_all=True)
    for pub_top in result:
        print(f"pub_top:{pub_top}")

if __name__ == "__main__":
    main()