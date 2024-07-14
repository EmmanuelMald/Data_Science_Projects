import apache_beam as beam

def create_lines(line:str)->list:
    
    return line.split(",")

with beam.Pipeline() as p:
    data = p | "importing_csv" >> beam.io.ReadFromText(r"C:\Users\Emmanuel\OneDrive - Instituto Politecnico Nacional\GITHUB\Data Science Projects\Data_Science_Projects\apache_beam\fulldata.csv")
    lines = data | beam.Map(create_lines)
    lines | "print" >> beam.Map(print)