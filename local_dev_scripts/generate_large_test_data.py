# Script to echo Postgres lines for garbage test data
# Useful for local development where you want to test with a large dataset
# Need to chunk inserts otherwise the query goes too large for the docker container to handle.

set_size = 100000
num_sets = 1000
set_num = 0
while set_num < num_sets:
    num = 0
    print(
        """
INSERT INTO public.users (id, hash_firstname, hash_lastname, gender)
    VALUES
    """
    )
    while num < set_size - 1:
        print(
            f"    ({set_num * set_size + num}, 'dsdssdgarbagefirst', 'dgsaggggdjj', 'male'),"
        )
        num = num + 1
    print(
        f"    ({set_num * set_size + num}, 'dsdssdgarbagefirst', 'dgsaggggdjj', 'male');"
    )
    set_num = set_num + 1
