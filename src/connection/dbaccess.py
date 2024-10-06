import sqlite3
import logging

class Connection:
    def _init_logger(self):
        handler = logging.FileHandler('/home/muri/Desktop/some_python/weatherapiproject/log/database_connection.log', mode='w')
        format = logging.Formatter('%(levelname)s:%(asctime)s:%(funcName)s->%(message)s')
        handler.setFormatter(format)
        self.logger.addHandler(handler)

    def __init__(self, database):
        self.database = database
        self.logger = logging.getLogger('database_connection')
        self.logger.setLevel(logging.INFO)
        self._init_logger()
        
    def _check_if_location_already_exists(self, location: str) -> bool:
        check_query = "SELECT * FROM locations WHERE location = (?)"
        try:
            self.cursor.execute(check_query, (location,))
        except sqlite3.Error as e:
            self.logger.warning(f"Could not perform search for location {location}")
            return False
        else:
            rows = self.cursor.fetchall()
            if rows:
                self.logger.info(f"Location {location} already exists")
                return True
        return False



    def add_location(self, location: str) -> bool:
        insert_query = "INSERT INTO locations (location) VALUES (?)"
        if not self._check_if_location_already_exists(location):
            try:
                self.cursor.execute(insert_query, (location,))
            except sqlite3.Error as e:
                self.logger.exception(f"Cannot add location: {str(e)}")
                return False
            else:
                if self.cursor.rowcount > 0:
                    self.logger.info(f"Location {location} was added")
                    return True
                else:
                    self.logger.warning(f"Location {location} already existing")
                    return False
        return False


    def _check_if_location_and_datetime_exists(self, location:str, datetime:str) -> bool:
        check_query = """SELECT * FROM weather w 
                        JOIN locations l ON w.location_id = l.id 
                        WHERE location = (?) AND datetime = (?)"""
        try:
            self.cursor.execute(check_query, (location,))
        except sqlite3.Error as e:
            self.logger.warning(f"Could not perform search for location {location} and datetime {datetime}")
            return False
        else:
            rows = self.cursor.fetchall()
            if rows:
                self.logger.info(f"Location {location} and datetime {datetime} already exist")
                return True
        return False

    def add_conditions(self, location: str, temperature: int, datetime: str) -> bool:
        if not self._check_if_location_and_datetime_exists(location, datetime):
            extract_location_id_query = "SELECT id FROM locations WHERE location=(?)"
            location_id = None
            try:
                self.cursor.execute(extract_location_id_query, (location,))
                location_id = self.cursor.fetchone()[0]
            except sqlite3.Error as e:
                self.logger.error(f"Error in fetching id for {location}")
                return False
            else:
                if location_id is not None:
                    self.logger.info(f"Fetched id for {location}")
                else:
                    self.logger.warning(f"No location {location} in the database")
                    return False
            
            insert_condition_query = """INSERT INTO weather (temperature, datetime, location_id)
                                        VALUES (?, ?, ?)"""
            try: 
                self.cursor.execute(insert_condition_query, (temperature, datetime, location_id))
            except sqlite3.Error as e:
                self.logger.exception(f"Error in inserting condition {str(e)} for location {location}")
                return False
            else:
                if self.cursor.rowcount > 0:
                    self.logger.info(f"Condtions for location {location} were added successfully")
                    return True
                else:
                    self.logger.warning(f"Conditions for location {location} were not added")
                    return False
        return False
    

    def see_all_conditions(self):
        select_all_query = """SELECT w.temperature, w.datetime, l.location FROM
                            weather w JOIN locations l ON w.location_id = l.id
                            """
        try:
            self.cursor.execute(select_all_query)
        except sqlite3.Error as e:
            self.logger.exception(f"Cannot fetch all details")
            print("Error in fetching")
            return
        else:
            rows = self.cursor.fetchall()
            if rows:
                self.logger.info("Data fetched successfully")
                for row in rows:
                    print(f"(temperature: {row[0]}, datetime: {row[1]}, location: {row[2]})")
            else:
                self.logger.info("No data in weather")
                print("No rows")

    def __enter__(self):
        try:
            self.connection = sqlite3.connect(self.database)
            self.cursor = self.connection.cursor()
        except Exception as e:
             self.logger.error(f'Error in creation: {str(e)}')
             raise Exception(str(e))
        else:
            self.cursor.execute("PRAGMA foreign_keys = ON")
            self.logger.info('Connection and cursor created successfully. Foreign keys enabled')

        try:
            create_locations_table_query = """
                        CREATE TABLE IF NOT EXISTS locations (
                           id INTEGER PRIMARY KEY,
                           location TEXT
                        )"""
            create_weather_table_query = """
                        CREATE TABLE IF NOT EXISTS weather (
                           id INTEGER PRIMARY KEY,
                           temperature INTEGER,
                           datetime TEXT,
                           location_id INTEGER,
                           FOREIGN KEY (location_id) REFERENCES locations(id)
                           ON DELETE CASCADE ON UPDATE NO ACTION
                        )"""
                    
            self.cursor.execute(create_locations_table_query)
            self.cursor.execute(create_weather_table_query)
        except sqlite3.Error as e:
            self.logger.error(f'Error in creating tables: {str(e)}')
            raise Exception(str(e))
        else:
            self.logger.info('Tables successfully created or are already existing')
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()
        self.logger.info('Changes commited. Connection and cursor closed succesfully')


