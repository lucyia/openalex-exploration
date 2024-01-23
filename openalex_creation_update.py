import json
from datetime import datetime
from pyalex import Works

from utils import get_connection


CONFIG = "config.json"
DEFAULT_DATE = "2010-01-01T00:00:00.000000"


def is_record_updated(record, last_update_date=None):
    """
    Returns whether a record's update or creation timestamp is later than given
    timestamp representing last update of the data.

    Parameters
    ----------
    record : dict
        Record containing timestamp values of updated_date and created_date.
    last_update_date : str
        ISO timestamp.

    Returns
    -------
    bool
        Returns whether record is considered updated or stale.
    """
    if not last_update_date:
        last_update_date = datetime.now().isoformat()

    return (
        datetime.fromisoformat(
            record["updated_date"]) > datetime.fromisoformat(last_update_date)
    ) or (
        datetime.fromisoformat(
            record["created_date"]) > datetime.fromisoformat(last_update_date)
    )


def get_query(filter_params, sample):
    """
    Creates OPENALEX query with given filters and sample, if enabled.

    Parameters
    ----------
    filter_params : dict
        Record containing timestamp values of updated_date and created_date.
    sample : dict
        Contains flag for sampling with size and seed for the API.

    Returns
    -------
    Query
        OPENALEX query.
    """
    query = Works().filter(**filter_params)

    if sample["enabled"]:
        query = query.sample(sample["size"], seed=sample["seed"])

    return query


def get_pager(query, pagination):
    """
    Creates pager from given OPENALEX query with given pagination.

    NOTE: Cursor pagination prefered but not supported in data samples,
    see https://docs.openalex.org/how-to-use-the-api/get-lists-of-entities/sample-entity-lists

    Parameters
    ----------
    query : Query
        Record containing timestamp values of updated_date and created_date.
    sample : dict
        Contains flag for sampling with size and seed for the API.

    Returns
    -------
    Pager
        OPENALEX pager.
    """
    return query.paginate(method="page", per_page=pagination["page_size"])


def run(filter_params, pagination, sample, db_name, works_name, meta_name):
    """
    Creates and updates MongoDB with records 
    Another collection storing the metadata about the runs is

    Parameters
    ----------
    filter_params : dict
        Filter parameters for API queries.
    pagination : dict
        Pagination for the API.
    sample : dict
        Sampling info for the API.
    db_name : str
        Name of the database.
    works_name : str
        Collection name for storing the results of the API.
    meta_name : str
        Collection name for storing the metadata info about the running of the updates.
    """
    try:
        db = get_connection(db_name)['db']
        works = db[works_name]
        meta = db[meta_name]

        query = get_query(filter_params, sample)
        pager = get_pager(query, pagination)

        is_log_empty = meta.count_documents({}) == 0

        latest_update_date = DEFAULT_DATE if is_log_empty else meta.find().limit(1).sort({
            "_id": -1}).next()["run_date"]

        responses = []

        for page in pager:
            # NOTE: 'from_updated_date' param requires paid premium API key to be able to get only list of updated works  # pylint: disable=line-too-long
            # to avoid getting all data on each request, see https://docs.openalex.org/api-entities/works/work-object  # pylint: disable=line-too-long
            updated_records = [
                record for record in page if is_record_updated(record, latest_update_date)
            ]

            for record in updated_records:
                response = works.replace_one(
                    {"id": record["id"]},
                    record,
                    upsert=True
                )

                responses.append(response)

        meta.insert_one({
            "run_date": datetime.now().isoformat(),
            "db_count": works.count_documents({}),
            "new": sum([not res.raw_result['updatedExisting'] for res in responses]),
            "modified": sum([r.modified_count for r in responses]),
        })

    except Exception as e:  # pylint: disable=broad-exception-caught
        meta.insert_one({
            "run_date": datetime.now().isoformat(),
            "error": e
        })


with open(CONFIG, encoding="utf-8") as file:
    config = json.load(file)

    run(config['filter'], config['pagination'],
        config['sample'], **config['db_config'])
