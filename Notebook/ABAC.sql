-- Databricks notebook source
-- MAGIC %md
-- MAGIC [Docs](https://docs.databricks.com/aws/ja/data-governance/unity-catalog/abac/tutorial)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # ABACとは
-- MAGIC
-- MAGIC - **機能概要**：行フィルターと列マスクの属性ベースのアクセス制御
-- MAGIC - **これまでの課題**：RBACではグループ（Role）ごとのアクセス制御が可能だが、テーブルごとにフィルタ関数を適用する必要があり、データアクセスの制御方法として手間がかかっていた
-- MAGIC - **ABACの利点**：データに対してタグを付与することで、タグが付与されているデータ全体に一括でフィルタ関数を適用できるようになり、テーブルごとのフィルタ設定が不要になる

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # シナリオ
-- MAGIC
-- MAGIC EU所属の社員は EU の顧客レコードや SSN にのみアクセスできるようにする

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 事前準備
-- MAGIC
-- MAGIC EU_employee グループを作成し、ユーザーを EU_employee に追加
-- MAGIC - [プロフィールアイコン] > [設定] > [IDとアクセス] > [グループ] > [新規追加] から EU_employee と入力し、グループを作成
-- MAGIC ![グループの作成](./images/create_group.png)
-- MAGIC - 作成したグループを選択し、[メンバーを追加] を選択し、自身のメールアドレスを入力
-- MAGIC ![メンバーの追加](./images/add_member.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1. 管理タグの作成
-- MAGIC
-- MAGIC 管理タグとは、タグを一元管理するための機能<br>
-- MAGIC 管理タグ自体の使用も権限制御が可能
-- MAGIC
-- MAGIC - Databricks ワークスペースで、カタログ をクリックします。
-- MAGIC - [ ガバナンス ] タブ > [ 管理タグ ] ボタンをクリックします。
-- MAGIC - [ 管理タグを作成] をクリックします。
-- MAGIC - タグキーに "pii_{your_name}" を入力します。
-- MAGIC - 管理タグの説明を入力します。
-- MAGIC - タグに使用できる値( ssn と address)を入力します。これらの値のみをこのタグキーに割り当てることができます。
-- MAGIC
-- MAGIC ![タグの作成](./images/create_tage.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2. 使用するカタログとスキーマの指定

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "handson", "Select Catalog")
-- MAGIC dbutils.widgets.text("schema", "abac", "Select Schema")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog = dbutils.widgets.get("catalog")
-- MAGIC schema = dbutils.widgets.get("schema")
-- MAGIC
-- MAGIC # 指定したカタログとスキーマがない場合は作成
-- MAGIC spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.sql(f"USE CATALOG {catalog}")
-- MAGIC spark.sql(f"USE SCHEMA {schema}")

-- COMMAND ----------

SELECT current_catalog(), current_schema();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3. 顧客テーブルを作成

-- COMMAND ----------

-- テーブル作成
CREATE TABLE IF NOT EXISTS profiles (
    First_Name STRING,
    Last_Name STRING,
    Phone_Number STRING,
    Address STRING,
    SSN STRING
)
USING DELTA;

-- データの追加
INSERT INTO profiles (First_Name, Last_Name, Phone_Number, Address, SSN)
VALUES
('John', 'Doe', '123-456-7890', '123 Main St, NY', '123-45-6789'),
('Jane', 'Smith', '234-567-8901', '456 Oak St, CA', '234-56-7890'),
('Alice', 'Johnson', '345-678-9012', '789 Pine St, TX', '345-67-8901'),
('Bob', 'Brown', '456-789-0123', '321 Maple St, FL', '456-78-9012'),
('Charlie', 'Davis', '567-890-1234', '654 Cedar St, IL', '567-89-0123'),
('Emily', 'White', '678-901-2345', '987 Birch St, WA', '678-90-1234'),
('Frank', 'Miller', '789-012-3456', '741 Spruce St, WA', '789-01-2345'),
('Grace', 'Wilson', '890-123-4567', '852 Elm St, NV', '890-12-3456'),
('Hank', 'Moore', '901-234-5678', '963 Walnut St, CO', '901-23-4567'),
('Ivy', 'Taylor', '012-345-6789', '159 Aspen St, AZ', '012-34-5678'),
('Liam', 'Connor', '111-222-3333', '12 Abbey Street, Dublin, Ireland EU', '111-22-3333'),
('Sophie', 'Dubois', '222-333-4444', '45 Rue de Rivoli, Paris, France Europe', '222-33-4444'),
('Hans', 'Müller', '333-444-5555', '78 Berliner Str., Berlin, Germany E.U.', '333-44-5555'),
('Elena', 'Rossi', '444-555-6666', '23 Via Roma, Milan, Italy Europe', '444-55-6666'),
('Johan', 'Andersson', '555-666-7777', '56 Drottninggatan, Stockholm, Sweden EU', '555-66-7777');

-- COMMAND ----------

-- 再実行する際は、ポリシーを無効化してから実行してください
-- ポリシーが有効のままだと、フィルタされた結果が返ってきます
SELECT * FROM profiles;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **全部で15行あります。この後ABACフィルタを適用して、表示されるデータ数がどのように変化するかを確認します。**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4. 管理タグをPII列に追加
-- MAGIC
-- MAGIC 作成した管理タグから、各列に対応する値を付与

-- COMMAND ----------

-- pii_{名前}の部分をご自身で設定した名前に変更してください

-- 管理タグの追加
ALTER TABLE profiles
ALTER COLUMN SSN
SET TAGS ('pii_tsuchiya' = 'ssn');

ALTER TABLE profiles
ALTER COLUMN Address
SET TAGS ('pii_tsuchiya' = 'address');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![タグ付与後](./images/add_tag.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 5. EUの住所判定を行う関数を作成
-- MAGIC
-- MAGIC 特定の文字列（本デモでは address 列の値）が、ヨーロッパまたは EU を含んでいないかを確認

-- COMMAND ----------

-- 住所がEUにあるかどうかを判定する関数
CREATE OR REPLACE FUNCTION is_eu_address(address STRING)
RETURNS BOOLEAN
RETURN (
    SELECT CASE
        WHEN LOWER(address) LIKE '%eu%'
          OR LOWER(address) LIKE '%e.u.%'
          OR LOWER(address) LIKE '%europe%'
        THEN TRUE
        ELSE FALSE
    END
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 6. 行フィルタポリシーを適用
-- MAGIC
-- MAGIC 5で作成した関数を用いて、EUに該当する行のみを表示するフィルタを適用
-- MAGIC
-- MAGIC - abac スキーマを選択します。
-- MAGIC - [ ポリシー ] タブをクリックします。
-- MAGIC - [新しいポリシー ] をクリックします。
-- MAGIC
-- MAGIC ![](./images/apply_policy.png)
-- MAGIC
-- MAGIC - [名前 ] に、ポリシーの名前 "allow_eu_data" を入力します。
-- MAGIC - プリンシパル では:
-- MAGIC   - [ 適用先... ] で、EU_employeeを選択します。
-- MAGIC   - [以下を除きます... ] を空白のままにします。
-- MAGIC - [目的] で、"テーブルの行を非表示にする" を選択します。
-- MAGIC
-- MAGIC ![](./images/apply_row_policy_1.png)
-- MAGIC
-- MAGIC - [条件] で既存のものを選択から、作成した `is_eu_address` 関数を選択します。
-- MAGIC - [関数パラメータ] で [Map column to parameter if it has a specific tag] を選択し、[pii_tsuchiya:address] を選択します。
-- MAGIC - 「 ポリシーの作成 」をクリックします。
-- MAGIC
-- MAGIC ![](./images/apply_row_policy_2.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # ポリシーの継承
-- MAGIC テーブルにもポリシーが継承されていることが確認できます。
-- MAGIC
-- MAGIC ![](./images/policy_inheritance.png)

-- COMMAND ----------

SELECT * FROM profiles;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **元々15行表示されていたテーブルのうち、住所がEUに該当する5行だけ表示されるようになりました。**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 7. SSNをマスクする関数の作成
-- MAGIC SSN（Social Security Number：社会保障番号）を非表示にするマスキング

-- COMMAND ----------

-- SSNのマスキングを行う関数
CREATE FUNCTION mask_SSN(ssn STRING)
RETURN '***-**-****';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 8. 列マスクを適用
-- MAGIC 6の手順と同様に、列マスクを適用（以下は除きますに設定している data_governance_admin は一例で今回は設定不要です。）
-- MAGIC
-- MAGIC ![](./images/apply_column_policy_1.png)
-- MAGIC ![](./images/apply_column_policy_2.png)

-- COMMAND ----------

SELECT * FROM profiles;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **SSNがマスキングされました。**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 9. 更なるガバナンス強化に向けて
-- MAGIC - ABACでデータガバナンスが簡素化されましたが、各データに対するタグの付与は依然として必要です。
-- MAGIC - このタグ付けを自動化するための機能として、[データの分類](https://docs.databricks.com/aws/ja/data-governance/unity-catalog/data-classification)という機能がBeta版として提供されています。
-- MAGIC - この機能を使用すると、エンジンはエージェント AI システムを使用して、Unity Catalog 内の任意のテーブルを自動的に分類し、タグ付けします。スキャンは増分的に行われ、手動で構成しなくてもすべての新しいデータが分類されるように最適化されます。
-- MAGIC - Catalog の詳細タブから機能を有効化可能です。(現状 Free Edition では使用不可です。)
-- MAGIC
-- MAGIC ![](./images/data_classification.png)
-- MAGIC
-- MAGIC - 分類結果をタグとして使用する際は、レビュー画面から自動タグ付けのトグルをONします。
-- MAGIC - データに誤ってタグが付けられた場合は、手動でタグを削除できます。 今後のスキャンではタグは再適用されません。

-- COMMAND ----------


