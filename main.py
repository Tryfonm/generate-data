import pandas as pd
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import create_engine


class GenerateData():

    def __init__(self) -> None:

        # TODO: Replace this part with a safer approach
        # by reading a file and handling the excepion
        SERVER = ''
        DATABASE = ''
        PORT = ''
        USERNAME = ''
        PASSWORD = ''
        DRIVER = 'SQL Server'
        conn_str = f'mssql+pymssql://{USERNAME}:{PASSWORD}@{SERVER}:{PORT}/{DATABASE}'
        self._engine = create_engine(conn_str)

    @property
    def engine(self):
        return self._engine

    @engine.setter
    def enginer(self, SERVER, DATABASE, PORT, USERNAME, PASSWORD):
        conn_str = f'mssql+pymssql://{USERNAME}:{PASSWORD}@{SERVER}:{PORT}/{DATABASE}'
        self._engine = create_engine(conn_str)

    def _get_model(self, schema='test', table='sequence_test', sequence_length=11):
        Session = sessionmaker(bind=self.engine)
        Base = declarative_base()

        class MyModel(Base):
            __tablename__ = table
            __table_args__ = {'schema': schema}
            id = Column(Integer, primary_key=True)
            for i in range(1, sequence_length):
                locals()[f'step_{i:03d}'] = Column(String)

        Base.metadata.create_all(self.engine)
        session = Session()
        session.commit()
        session.close()

        return MyModel

    def inject_in_batches(self, df, batch_size=10, schema='test', table='sequence_test'):

        Session = sessionmaker(bind=self.engine)
        session = Session()

        Model = self._get_model(
            schema='test', table='sequence_test', sequence_length=11)

        for i in range(0, len(df), batch_size):
            try:
                chunk = df[i:i + batch_size]
                session.bulk_insert_mappings(
                    Model, chunk.to_dict(orient='records'))
                session.commit()
            except Exception as e:
                print(f'Exception {e}')
        session.close()

    @staticmethod
    def preprocess_dataset(df, sequence_length=101):
        temp_df = pd.DataFrame()
        for i in range(1, sequence_length):
            temp_df[f'step_{i:03d}'] = df['list_of_sentences'].apply(
                lambda x: x[:i])
        temp_df = temp_df.applymap(lambda x: ".".join(x) + ".")

        return temp_df
