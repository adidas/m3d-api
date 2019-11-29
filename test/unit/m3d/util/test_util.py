import pytest
from mock import patch, call

from m3d.util.util import Util


class TestUtil(object):

    @pytest.mark.bdp
    @patch('os.system', return_value=0)
    def test_send_mail(self, os_system_patch):
        Util.send_email(["firstname.lastname@adidas.com"], "hello", "hello")
        os_system_patch.assert_has_calls([
            call('echo "hello" | mailx -s "hello" firstname.lastname@adidas.com')
        ])

    @pytest.mark.bdp
    def test_get_target_partitions_list(self):
        """
        This method tests the correct functionality of get_partition_column_list of Util class
        :return:
        """
        assert Util.get_target_partitions_list("year") == ["year"]
        assert Util.get_target_partitions_list("month") == ["year", "month"]
        assert Util.get_target_partitions_list("day") == ["year", "month", "day"]
        assert Util.get_target_partitions_list("") == []

        with pytest.raises(Exception) as exc_info:
            Util.get_target_partitions_list("country")
        assert "Partition type country not supported" in str(exc_info.value)

    @pytest.mark.bdp
    def test_get_target_partitions_string(self):
        """
        This method tests the correct functionality of get_partition_column_string of Util class
        :return:
        """
        assert Util.get_target_partitions_string("year") == "year"
        assert Util.get_target_partitions_string("month") == "year,month"
        assert Util.get_target_partitions_string("day") == "year,month,day"
        assert Util.get_target_partitions_string("") == ""

        with pytest.raises(Exception) as exc_info:
            Util.get_target_partitions_list("country")
        assert "Partition type country not supported" in str(exc_info.value)

    @pytest.mark.bdp
    def test_get_defined_partition_columns_hive(self):
        """
        This method tests the correct functionality of get_partition_column_string of Util class
        :return:
        """
        assert Util.get_defined_target_partitions_hive("year") == "year smallint"
        assert Util.get_defined_target_partitions_hive("month") == "year smallint,month smallint"
        assert Util.get_defined_target_partitions_hive("day") == "year smallint,month smallint,day smallint"
        assert Util.get_defined_target_partitions_hive("") == ""

        with pytest.raises(Exception) as exc_info:
            Util.get_target_partitions_list("country")
        assert "Partition type country not supported" in str(exc_info.value)

    @pytest.mark.bdp
    def test_oracle_view_to_hive_view(self):
        oracle_view_ddl = \
            "\n  CREATE OR REPLACE FORCE EDITIONABLE VIEW \"MART_MOD\".\"TEST_VIEW\" (" + \
            "" + \
            "\"GENDER\", \"GROUP_ARTICLE\", \"BRAND\", \"GROUP_MODEL\", " + \
            "\"RMH_PRODUCT_DIVISION\", \"RMH_GENDER\", \"RMH_CATEGORY\", \"RMH_PRODUCT_TYPE\", " + \
            "\"BUSINESS_SEGMENT\", \"BUSINESS_UNIT\", \"COLORWAY_NAME\", \"SEASON_ACTIVE\", " + \
            "\"SEASON_CREATE\", \"SIZE_PAGE\", \"KEY_CATEGORY\", \"SUB_BRAND\", " + \
            "\"CORPORATE_MARKETING_LINE\", \"PRODUCT_DIVISION\", \"ORDER_LOCKED\", \"PRODUCT_GROUP\", " + \
            "\"PRODUCT_TYPE\", \"SPORTS_CATEGORY\", \"SOURCING_SIZE_SCALE\", \"RMH_RETAIL_CLASS\", " + \
            "\"RMH_RETAIL_DEPARTMENT\", \"RMH_RETAIL_SUB_CLASS\", \"RMH_RETAIL_SUB_DEPT\", \"RMH_RETAIL_SECTION\", " + \
            "\"AGE_GROUP\", \"ALTERNATE_ARTICLE\", \"ARTICLE_TYPE\", \"COLORWAY_LONG_DESCR\", " + \
            "\"COLORWAY_SHORT_DESCR\", \"LIFECYCLE_STATUS_DATE\", \"ORIGINAL_ARTICLE\", \"ARTICLE_DESCR\", " + \
            "\"VENDOR_ARTICLE\", \"SALES_LINE\", \"CATEGORY_MARKETING_LINE\"" + \
            "" + \
            ") AS \n  SELECT \n" + \
            "" + \
            "gender,\ngroup_article,\nbrand,\ngroup_model,\n" + \
            "rmh_product_division,\nrmh_gender,\nrmh_category,\nrmh_product_type,\n" + \
            "business_segment,\nbusiness_unit,\ncolorway_name,\nseason_active,\n" + \
            "season_create,\nsize_page,\nkey_category,\nsub_brand,\n" + \
            "corporate_marketing_line,\nproduct_division,\norder_locked,\nproduct_group,\n" + \
            "product_type,\nsports_category,\nsourcing_size_scale,\nrmh_retail_class,\n" + \
            "rmh_retail_department,\nrmh_retail_sub_class,\nrmh_retail_sub_dept,\nrmh_retail_section,\n" + \
            "age_group,\nalternate_article,\narticle_type,\ncolorway_long_descr,\n" + \
            "colorway_short_descr,\nlifecycle_status_date,\noriginal_article,\narticle_descr,\n" + \
            "vendor_article,\nSALES_LINE,\n" + \
            "category_marketing_line\n" + \
            "" + \
            "FROM \n" + \
            "lake_out.bi_test_view"

        hive_view_ddl = Util.oracle_view_to_hive_view(oracle_view_ddl)

        expected_hive_ddl = \
            "CREATE VIEW `MART_MOD`.`TEST_VIEW` " + \
            "(" + \
            "`GENDER`, `GROUP_ARTICLE`, `BRAND`, `GROUP_MODEL`, " + \
            "`RMH_PRODUCT_DIVISION`, `RMH_GENDER`, `RMH_CATEGORY`, `RMH_PRODUCT_TYPE`, " + \
            "`BUSINESS_SEGMENT`, `BUSINESS_UNIT`, `COLORWAY_NAME`, `SEASON_ACTIVE`, " + \
            "`SEASON_CREATE`, `SIZE_PAGE`, `KEY_CATEGORY`, `SUB_BRAND`, " + \
            "`CORPORATE_MARKETING_LINE`, `PRODUCT_DIVISION`, `ORDER_LOCKED`, `PRODUCT_GROUP`, " + \
            "`PRODUCT_TYPE`, `SPORTS_CATEGORY`, `SOURCING_SIZE_SCALE`, `RMH_RETAIL_CLASS`, " + \
            "`RMH_RETAIL_DEPARTMENT`, `RMH_RETAIL_SUB_CLASS`, `RMH_RETAIL_SUB_DEPT`, `RMH_RETAIL_SECTION`, " + \
            "`AGE_GROUP`, `ALTERNATE_ARTICLE`, `ARTICLE_TYPE`, `COLORWAY_LONG_DESCR`, " + \
            "`COLORWAY_SHORT_DESCR`, `LIFECYCLE_STATUS_DATE`, `ORIGINAL_ARTICLE`, `ARTICLE_DESCR`, " + \
            "`VENDOR_ARTICLE`, `SALES_LINE`, `CATEGORY_MARKETING_LINE`" + \
            ") " + \
            "AS SELECT " + \
            "gender, group_article, brand, group_model, " + \
            "rmh_product_division, rmh_gender, rmh_category, rmh_product_type, " + \
            "business_segment, business_unit, colorway_name, season_active, " + \
            "season_create, size_page, key_category, sub_brand, " + \
            "corporate_marketing_line, product_division, order_locked, product_group, " + \
            "product_type, sports_category, sourcing_size_scale, rmh_retail_class, " + \
            "rmh_retail_department, rmh_retail_sub_class, rmh_retail_sub_dept, rmh_retail_section, " + \
            "age_group, alternate_article, article_type, colorway_long_descr, " + \
            "colorway_short_descr, lifecycle_status_date, original_article, article_descr, " + \
            "vendor_article, SALES_LINE, category_marketing_line " + \
            "FROM lake_out.bi_test_view"

        assert hive_view_ddl == expected_hive_ddl
