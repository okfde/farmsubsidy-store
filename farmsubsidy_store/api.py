from flask import Flask, request
from flask_caching import Cache
from flask_restful import Api, Resource
from followthemoney.util import make_entity_id as make_id
from furl import furl

from farmsubsidy_store import search, settings, views
from farmsubsidy_store.logging import get_logger

log = get_logger(__name__)


cache = Cache(
    config={
        "CACHE_TYPE": settings.API_CACHE_TYPE,
        "CACHE_DEFAULT_TIMEOUT": settings.API_CACHE_DEFAULT_TIMEOUT,
        "CACHE_KEY_PREFIX": "fsapi",
        "CACHE_REDIS_URL": settings.REDIS_URL,
    }
)


class ApiView(views.BaseListView, Resource):
    def get_page_url(self, change: int):
        new_page = self.page + change
        url = furl(request.url)
        url.args["p"] = new_page
        return str(url)

    def get(self):
        query = self.get_query(**request.args)
        cache_key = make_id(str(query))
        cached_results = cache.get(cache_key)
        if cached_results:
            log.info(f"Cache hit for `{cache_key}`")
            return cached_results

        self.get_results(**request.args)
        results = {
            "page": self.page,
            "item_count": self.query.count,
            "next_url": self.get_page_url(1) if self.has_next else None,
            "prev_url": self.get_page_url(-1) if self.has_prev else None,
            "schema": self.get_schema(),
            "results": [i.dict() for i in self.data],
        }
        cache.set(cache_key, results)
        return results

    @classmethod
    def get_schema(cls):
        return {
            "model": cls.model.schema(),
            "params": cls.params_cls.schema(),
        }


class PaymentApiView(views.PaymentListView, ApiView):
    endpoint = "/payments"


class RecipientApiView(views.RecipientListView, ApiView):
    endpoint = "/recipients"


class RecipientBaseApiView(views.RecipientBaseView, ApiView):
    endpoint = "/recipients/base"


class RecipientSearchApiView(search.RecipientSearchView, ApiView):
    endpoint = "/recipients/search"


class SchemeApiView(views.SchemeListView, ApiView):
    endpoint = "/schems"


class SchemeSearchApiView(search.SchemeSearchView, ApiView):
    endpoint = "/schemes/search"


class CountryApiView(views.CountryListView, ApiView):
    endpoint = "/countries"


class YearApiView(views.YearListView, ApiView):
    endpoint = "/years"


API_VIEWS = (
    PaymentApiView,
    RecipientApiView,
    RecipientBaseApiView,
    RecipientSearchApiView,
    SchemeApiView,
    SchemeSearchApiView,
    CountryApiView,
    YearApiView,
)


class ApiSchemeView(Resource):
    def get(self):
        return {
            "endpoints": [
                {"url": cls.endpoint, "scheme": cls.get_schema()} for cls in API_VIEWS
            ]
        }


app = Flask(__name__)
api = Api(app)
cache.init_app(app)


api.add_resource(ApiSchemeView, "/")

for view in API_VIEWS:
    api.add_resource(view, view.endpoint)

if __name__ == "__main__":
    app.run(debug=settings.FLASK_DEBUG)
