from mrjob.job import MRJob

class MRMoviesReviewed(MRJob):

    def mapper(self, _, line):
        (userID,movieID,rating,timestamp) = line.split(',')
        yield userID, movieID

    def combiner(self, userID, movies):
        yield userID, len(list(movies))

    def reducer(self, userID, count):
        yield userID, sum(count)

if __name__ == '__main__':
    MRMoviesReviewed.run()