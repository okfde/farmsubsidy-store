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
            "results": [i.dict() for i in self.data],
        }
        cache.set(cache_key, results)
        return results


def get_api_view(view_cls):
    return type(f"{view_cls.__name__}ApiView", (view_cls, ApiView), {})


app = Flask(__name__)
api = Api(app)
cache.init_app(app)


api.add_resource(get_api_view(views.PaymentListView), "/payments")
api.add_resource(get_api_view(views.RecipientListView), "/recipients")
api.add_resource(get_api_view(views.RecipientBaseView), "/recipients_base")
api.add_resource(get_api_view(search.RecipientSearchView), "/recipients/search")
api.add_resource(get_api_view(views.SchemeListView), "/schemes")
api.add_resource(get_api_view(search.SchemeSearchView), "/schemes/search")
api.add_resource(get_api_view(views.CountryListView), "/countries")
api.add_resource(get_api_view(views.YearListView), "/years")


if __name__ == "__main__":
    app.run(debug=settings.FLASK_DEBUG)
