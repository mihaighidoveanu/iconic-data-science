#!/usr/bin/python
import psycopg2
import psycopg2.extras
import csv
import sys

def main():
    conn = None
    try:
        conn = psycopg2.connect(host="localhost", dbname="mag", user="iconic", password="iconic")

        author = sys.argv[1]

        network = dict()
        coauthors_years_list = []
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                        "SELECT author2, year "
                        "FROM author_network "
                        "WHERE author1 = %s "
                        , (author,))
                coauthors_years_list = cursor.fetchall()

        coauthors_dict = dict(coauthors_years_list)

        coauthors = [a for a,year in coauthors_years_list]

        for coauthor1, year1 in coauthors_years_list:
            if year1 not in network:
                network[year1] = []
            network[year1].append((author, coauthor1))

        for coauthor1, year1 in coauthors_years_list:
            coauthors2_years_list = []
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                            "SELECT author2, year "
                            "FROM author_network "
                            "WHERE author1 = %s "
                            "AND author2 > author1 "
                            "AND author2 = ANY(%s);",
                            (coauthor1,coauthors))
                    coauthors2_years_list = cursor.fetchall()

            for coauthor2,year2 in coauthors2_years_list:
                year = max(year1, year2, coauthors_dict[coauthor2])
                if year not in network:
                    network[year] = []
                network[year].append((coauthor1, coauthor2))


        # close the communication with the PostgreSQL
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    for year,rel in sorted(network.items()):
        print year,":"
        for (a1,a2) in rel:
            if a1 == author:
                print ("\tNode {}".format(a2))
            else:
                print ("\tEdge ({},{})".format(a1,a2))


if __name__ == '__main__':
    main()
