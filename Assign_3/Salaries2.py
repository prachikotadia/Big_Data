from mrjob.job import MRJob

class MRSalaries2(MRJob):

    def mapper(self, _, line):
        # Split the input line into fields
        fields = line.split('\t')
        # Extract the annual salary from the fields
        annual_salary = float(fields[5])
        
        # Determine the salary group based on the annual salary
        if annual_salary >= 100000.00:
            salary_group = 'High'
        elif 50000.00 <= annual_salary <= 99999.99:
            salary_group = 'Medium'
        else:
            salary_group = 'Low'
        
        # Emit the salary group as the key and a count of 1 as the value
        yield salary_group, 1

    def combiner(self, salary_group, counts):
        # Sum up the counts for each salary group
        yield salary_group, sum(counts)

    def reducer(self, salary_group, counts):
        # Sum up the counts for each salary group and yield the result
        yield salary_group, sum(counts)


if __name__ == '__main__':
    MRSalaries2.run()
