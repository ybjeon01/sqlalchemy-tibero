from sqlalchemy.testing import mock

from sqlalchemy.testing.suite import *
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest


from sqlalchemy.engine import ObjectScope
from sqlalchemy.engine import ObjectKind

# TODO: 티베로 데이터베이스에서 하면 안되는 테스트가 있는지 확인하기


class ComponentReflectionTest(_ComponentReflectionTest):
    def exp_ccs(
        self,
        schema=None,
        scope=ObjectScope.ANY,
        kind=ObjectKind.ANY,
        filter_names=None,
    ):
        class tt(str):
            def __eq__(self, other):
                res = (
                    other.lower()
                    .replace("(", "")
                    .replace(")", "")
                    .replace("`", "")
                )
                return self in res

        def cc(text, name, comment=None):
            return {"sqltext": tt(text), "name": name, "comment": comment}

        # print({1: "test2 > (0)::double precision"} == {1: tt("test2 > 0")})
        # assert 0
        materialized = {(schema, "dingalings_v"): []}
        views = {
            (schema, "email_addresses_v"): [],
            (schema, "users_v"): [],
            (schema, "user_tmp_v"): [],
        }
        self._resolve_views(views, materialized)
        tables = {
            (schema, "users"): [
                cc(
                    "test2 > 0",
                    "zz_test2_gt_zero",
                    comment="users check constraint",
                ),
                cc("test2 <= 1000", mock.ANY),
            ],
            (schema, "dingalings"): [
                cc(
                    "address_id > 0 and address_id < 1000",
                    name="address_id_gt_zero",
                ),
            ],
            (schema, "email_addresses"): [],
            (schema, "comment_test"): [],
            (schema, "no_constraints"): [],
            (schema, "local_table"): [],
            (schema, "remote_table"): [],
            (schema, "remote_table_2"): [],
            (schema, "noncol_idx_test_nopk"): [],
            (schema, "noncol_idx_test_pk"): [],
            (schema, self.temp_table_name()): [],
        }
        res = self._resolve_kind(kind, tables, views, materialized)
        res = self._resolve_names(schema, scope, filter_names, res)
        return res