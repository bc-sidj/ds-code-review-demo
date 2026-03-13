import logging


def get_file_hash(local_filepath, delete_file=False):
    """
    This function will return a 64 byte file hash using the blake2b algorithm.
    :param local_filepath: the path to the file to hash
    :param delete_file: if True, the file will be deleted after hashing
    :return: the hash of the file
    """
    import hashlib
    import os

    hash_blake2b = hashlib.blake2b()
    with open(local_filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_blake2b.update(chunk)

    if delete_file:
        os.remove(local_filepath)

    return hash_blake2b.hexdigest()


def unload(connection_id, db_object, schema, prefix, next_ds, filter, stage, max_file_size, skip_on_missing_file=False, date_prefix_flag=True):
    """
    function that unloads tables to s3
    :param ds execution date
    :param db_object name of object to be unloaded
    :param schema db_object's schema
    :param filter to be applied to db_object
    """
    from include.utils.db_utils import export_to_parquet
    from include.utils.utils import check_stage_path

    # Export the data:
    if date_prefix_flag:
        unload_path = f'@{stage}/{prefix}/{next_ds}/data.parquet'
    else:
        unload_path = f'@{stage}/{prefix}/data.parquet'
    unload_object = f'{schema}.{db_object}'
    export_to_parquet(connection_id, unload_object, unload_path, filter, max_file_size)

    # make sure that we actually exported a file:
    if skip_on_missing_file:
        success = check_stage_path(connection_id, unload_path, no_exception=True)
        if not success:
            logging.warning(f"Exception for {db_object} marked to be skipped.")
    else:  # it should have failed earlier, but just to be safe:
        check_stage_path(connection_id, unload_path)
