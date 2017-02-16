import logging, argparse, psycopg2

# Set the log output file, and the log level
logging.basicConfig(filename="snippets.log", level=logging.DEBUG)
logging.debug("Connecting to PostgreSQL")
connection = psycopg2.connect(database="snippets")
logging.debug("Database connection established.")

def put(name, snippet, hidden):
    """Store a snippet with an associated name."""
    logging.info("Storing snippet {!r}: {!r}".format(name, snippet))
    with connection, connection.cursor() as cursor:
        try:
            command = "insert into snippets values (%s, %s, %s);"
            cursor.execute(command, (name, snippet, hidden))
        except psycopg2.IntegrityError as e:
            connection.rollback()
            if hidden:
                command = "update snippets set message=%s where keyword=%s and hidden"
            else:
                command = "update snippets set message=%s where keyword=%s and not hidden"
            cursor.execute(command, (snippet, name, hidden))
    logging.debug("Snippet stored successfully.")
    return name, snippet
    
def get(name):
    """Retrieve the snippet with a given name."""
    logging.info("Retrieving snippet get({!r})".format(name))
    with connection, connection.cursor() as cursor:
        cursor.execute("select message from snippets where keyword=%s", (name,))
        row = cursor.fetchone()
    logging.debug("Snippet received successfully.")
    if row: 
        return (name, row[0])
    else:
        return (name, None)
        
def catalog(name):
    logging.info("Retrieving Catalog ordered by ({!r})".format(name))
    with connection, connection.cursor() as cursor:
        stmt = "select * from snippets where hidden is false order by " + name
        cursor.execute(stmt)
        rows = cursor.fetchall()
    logging.debug("Catalog received successfully.")    
    return rows
    
def search(name):
    logging.info("Searched catalog using {!r}".format(name))
    with connection, connection.cursor() as cursor:
        stmt1= "select * from snippets where message ~* '" + name + "' and hidden is false"
        cursor.execute(stmt1)
        rows = cursor.fetchall()
    logging.debug("Search successful.")    
    return rows
    
    

def main():
    """Main function"""
    logging.info("Constructing parser")
    parser = argparse.ArgumentParser(description="Store and retrieve snippets of text")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Subparser for the put command
    logging.debug("Constructing put subparser")
    put_parser = subparsers.add_parser("put", help="Store a snippet")
    put_parser.add_argument("name", help="Name of the snippet")
    put_parser.add_argument("snippet", help="Snippet text")
    put_parser.add_argument("--hidden", help="hidden unless specifically requested", action = "store_true")

    # Subparser for the get command
    logging.debug("Constructing get subparser")
    get_parser = subparsers.add_parser("get", help="Retrieve a snippet")
    get_parser.add_argument("name", help="Name of the snippet")
    
    #Subparser for the catalog command
    logging.debug("Constructing catalog subparser")
    catalog_parser = subparsers.add_parser("catalog", help="Retrieve a catalog of snippets")
    catalog_parser.add_argument("name", help="name of the snippet")
    
    #Subparser for the search command
    logging.debug("Constructing catalog subparser")
    search_parser = subparsers.add_parser("search", help="Searching through snippets using keyword")
    search_parser.add_argument("name", help="keyword of search")
    
    arguments = parser.parse_args()
    # Convert parsed arguments from Namespace to dictionary
    arguments = vars(arguments)
    command = arguments.pop("command")

    if command == "put":
        name, snippet = put(**arguments)
        print("Stored {!r} as {!r}".format(snippet, name))
    elif command == "get":
        snippet = get(**arguments)
        print("Retrieved snippet: {!r}".format(snippet))
    elif command == "catalog":
        name = catalog(**arguments)
        print("Retrieved catalog ordered by {!r}".format(name))
    elif command == "search":
        name = search(**arguments)
        print("Searched catalog using {!r}".format(name))

if __name__ == "__main__":
    main()