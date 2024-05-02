""" search business logic implementations """
import logging
from datetime import datetime, date

from django.conf import settings

from .filter_generator import SearchFilterGenerator
from .search_engine_base import SearchEngine
from .result_processor import SearchResultProcessor
from .utils import DateRange

# Default filters that we support, override using COURSE_DISCOVERY_FILTERS setting if desired
DEFAULT_FILTER_FIELDS = ["org", "modes", "language"]
log = logging.getLogger(__name__)

def course_discovery_filter_fields():
    """
    Look up the desired list of course discovery filter fields.
    """
    return getattr(settings, "COURSE_DISCOVERY_FILTERS", DEFAULT_FILTER_FIELDS)


def course_discovery_aggregations():
    """
    Discovery aggregations to include bucket names.

    By default we specify each filter field with unspecified size attribute.
    """
    return getattr(
        settings,
        "COURSE_DISCOVERY_AGGREGATIONS",
        {field: {} for field in course_discovery_filter_fields()}
    )


class NoSearchEngineError(Exception):
    """
    NoSearchEngineError exception.

    It is thrown if no search engine is specified.
    """


def perform_search(
        search_term,
        user=None,
        size=10,
        from_=0,
        course_id=None):
    """
    Call the search engine with the appropriate parameters
    """
    # field_, filter_ and exclude_dictionary(s) can be overridden by calling application
    # field_dictionary includes course if course_id provided
    (field_dictionary, filter_dictionary, exclude_dictionary) = SearchFilterGenerator.generate_field_filters(
        user=user,
        course_id=course_id
    )

    searcher = SearchEngine.get_search_engine(
        getattr(settings, "COURSEWARE_CONTENT_INDEX_NAME", "courseware_content")
    )
    if not searcher:
        raise NoSearchEngineError("No search engine specified in settings.SEARCH_ENGINE")

    results = searcher.search_string(
        search_term,
        field_dictionary=field_dictionary,
        filter_dictionary=filter_dictionary,
        exclude_dictionary=exclude_dictionary,
        size=size,
        from_=from_,
    )

    # post-process the result
    for result in results["results"]:
        result["data"] = SearchResultProcessor.process_result(result["data"], search_term, user)

    results["access_denied_count"] = len([r for r in results["results"] if r["data"] is None])
    results["results"] = [r for r in results["results"] if r["data"] is not None]

    return results


def course_discovery_search(search_term=None, size=20, from_=0, field_dictionary=None):
    """
    Course Discovery activities against the search engine index of course details
    """
    use_search_fields = ["org"]
    (search_fields, _, exclude_dictionary) = SearchFilterGenerator.generate_field_filters()
    use_field_dictionary = {field: search_fields[field] for field in search_fields if field in use_search_fields}
    log.info("field_dictionary------ %s", field_dictionary)
    log.info("exclude_dictionary------ %s", exclude_dictionary)
    if field_dictionary:
        use_field_dictionary.update(field_dictionary)
    log.info("use_field_dictionary------------ %s", use_field_dictionary)
    today = datetime.utcnow()
    if "estatus" in use_field_dictionary:
        status_value = use_field_dictionary.pop("estatus")
        log.info("status_value-------- %s", status_value)        
        exclude_dictionary["invitation_only"] = False
        if status_value == "ongoing":
            use_field_dictionary["start"] = DateRange(None, today)
            use_field_dictionary["end"] = DateRange(today, None)
        elif status_value == "upcoming":
            use_field_dictionary["start"] = DateRange(today, None)
        elif status_value == "finished":
            use_field_dictionary["end"] = DateRange(None, today)

        # Handle invitation_only as a potentially additional condition.
        if status_value == "invitation_only" or 'invitation_only' in status_value.split():
            use_field_dictionary["invitation_only"] = True
    
    if not getattr(settings, "SEARCH_SKIP_ENROLLMENT_START_DATE_FILTERING", False):
        use_field_dictionary["enrollment_start"] = DateRange(None, datetime.utcnow())
    
    searcher = SearchEngine.get_search_engine(
        getattr(settings, "COURSEWARE_INFO_INDEX_NAME", "course_info")
    )
    if not searcher:
        raise NoSearchEngineError("No search engine specified in settings.SEARCH_ENGINE")
    log.info("use_field_dictionary--------- %s", use_field_dictionary)
    results = searcher.search(
        query_string=search_term,
        size=size,
        from_=from_,
        field_dictionary=use_field_dictionary,
        filter_dictionary={"enrollment_end": DateRange(datetime.utcnow(), None)},
        exclude_dictionary=exclude_dictionary,
        aggregation_terms=course_discovery_aggregations(),
    )
    
    return results
