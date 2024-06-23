import xml.etree.ElementTree as ET
import psycopg2
from config import host, user, password, db_name

def add_master(uniqueid, linkurl, genre, type_book):
    cursor.execute(                      
            f"""INSERT INTO master VALUES ({uniqueid}, '{linkurl}','{genre}','{type_book}');"""
        )

def add_authors(authors, uniqueid):
    if authors != {}:
        for i in authors.keys():
            # i = authorid
            lastname = authors[i]['lastname']
            if lastname!=None:
                lastname = lastname.replace("'", " ")
            initials = authors[i]['initials']
            if lastname!=None:
                lastname = lastname.replace("'", ".")
            cursor.execute(                      
                    f"""INSERT INTO authors VALUES ({i}, {uniqueid},'{lastname}','{initials}');"""
                )

def add_affiliations(affiliations):
    if affiliations != {}:
        for i in affiliations.keys():
        # i = authorid
            orgid = affiliations[i]['orgid']
            orgname = affiliations[i]['orgname']
            if orgname!=None:
                orgname = orgname.replace("'", '"')
            cursor.execute(                      
                    f"""INSERT INTO affiliations (orgid, orgname, authorid) VALUES ({orgid}, '{orgname}', {i});"""
                )


def add_abstracts(uniqueid, abstracts):
    if abstracts != []:
        for i in abstracts:
            if i!=None:
                i = i.replace("'", " ")
            cursor.execute(                      
                    f"""INSERT INTO abstracts VALUES ({uniqueid}, '{i}');"""
                )

def add_keywords(uniqueid, keywords):
    if keywords != []:
        for i in keywords:
            if i!=None:
                i = i.replace("'", " ")
            cursor.execute(                      
                    f"""INSERT INTO keywords VALUES ({uniqueid}, '{i}');"""
                )

def add_references(uniqueid, reference):
    if reference != []:
        for i in reference:
            if i!=None:
                i = i.replace("'", " ")
            cursor.execute(                      
                    f"""INSERT INTO reference VALUES ({uniqueid}, '{i}');"""
                )

def add_titles(uniqueid, titles):
    if titles != []:
        for i in titles:
            if i!=None:
                i = i.replace("'", " ")
            cursor.execute(                      
                    f"""INSERT INTO titles VALUES ({uniqueid}, '{i}');"""
                )

def add_codes(uniqueid, codes):
    if codes != []:
        for i in codes:
            if i!=None:
                i = i.replace("'", " ")
            cursor.execute(                      
                    f"""INSERT INTO cods VALUES ({uniqueid}, '{i}');"""
                )
def check_authors(id_to_check):
    # Выполнение SQL-запроса для проверки наличия ID в таблице
    cursor.execute(f"SELECT EXISTS(SELECT 1 FROM authors WHERE authorid = {id_to_check})")
    exists = cursor.fetchone()[0]
    return exists



tree = ET.parse('data2.xml')
root = tree.getroot()


try:
    connection = psycopg2.connect(
        host = host,
        user = user,
        password = password,
        database = db_name
    )
    connection.autocommit = True

    with connection.cursor() as cursor:
        # Поиск по всем книгам 
        for child in root:
            uniqueid = child.attrib['id']
            link = child.find('linkurl').text
            genre = child.find('genre').text
            type_book = child.find('type').text
            titles = []

            for i in child.find('titles'):
                titles.append(i.text)

            authors = {}
            affiliations = {}
            if child.find('authors') != None:
                for i in child.find('authors'):
                    if i.find('authorid') != None:
                        authorid = i.find('authorid').text
                        if not check_authors(authorid):
                            authors[i.find('authorid').text] = {'lastname' :  i.find('lastname').text if i.find('lastname') != None else None, 'initials' : i.find('initials').text if i.find('initials') != None else None}
                            

                    if i.find('affiliations') != None:
                        for j in i.find('affiliations'):
                            affiliations[authorid] = {'orgid' : j.find('orgid').text if j.find('orgid') != None else 'NULL', 'orgname' : j.find('orgname').text if j.find('orgname') != None else None }

            abstracts = []
            if child.find('abstracts') != None:
                for i in child.find('abstracts'):
                    abstracts.append(i.text)
            

            codes = []
            if child.find('codes') != None:
                for i in child.find('codes'):
                    codes.append(i.text)
            
                    
            references = []
            if child.find('references') != None:
                for i in child.find('references'):
                    references.append(i.text)
            

            keywords = []
            if child.find('keywords') != None:
                for i in child.find('keywords'):
                    keywords.append(i.text)
           
           
                
            add_keywords(uniqueid=uniqueid, keywords=keywords) # 3-ий запуск
            add_references(uniqueid=uniqueid, reference=references) # 3-ий запуск
            add_abstracts(uniqueid=uniqueid, abstracts=abstracts) # 3-ий запуск
            add_codes(uniqueid=uniqueid, codes=codes) # 3-ий запуск
            #add_master(uniqueid=uniqueid, linkurl=link, genre=genre, type_book=type_book) # 1-ый запуск
            add_titles(uniqueid=uniqueid, titles=titles) # 3-ий запуск
            #add_authors(uniqueid=uniqueid, authors=authors) # 2-ой запуск
            add_affiliations(affiliations=affiliations) # 3-ий запуск

    
    print("Row added.")
        
except Exception as _ex:
    print("Error to PostgreSQL: ", _ex)
finally:
    if connection:
        connection.close()
        print("PostgreSQL connection closed.")




