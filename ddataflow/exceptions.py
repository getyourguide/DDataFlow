class BiggerThanMaxSize(Exception):
    def create(self, datasource_name, size_estimation, size_limit):
        return BiggerThanMaxSize(
            f"DF {datasource_name} is estimated to be {round(size_estimation)}GB large. It is larger than the "
            f"current"
            f" limit {size_limit}GB. Adjust sampling filter or set higher limit"
        )


class WritingToLocationDenied(Exception):
    def create(self, base_folder):
        return WritingToLocationDenied(f"Only writing to {base_folder} is allowed")


class WriterNotFoundException(Exception):
    def create(self, name: str):
        return WriterNotFoundException(
            f"The data writer was not found in your  config: {name}"
        )
