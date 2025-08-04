import axios from "axios";

const URL = "http://localhost:8081/api/integration/";

const testClickhouseConnection = (clickhouseConfig) => {
  return axios
    .post(`${URL}clickhouse/test-connection`, clickhouseConfig)
    .then((res) => res.data)
    .catch((err) => {
      throw err;
    });
};

const fetchClickhouseTables = (clickhouseConfig) => {
  return axios
    .post(`${URL}clickhouse/tables`, clickhouseConfig)
    .then((res) => res.data)
    .catch((err) => {
      throw err;
    });
};

const fetchClickhouseSchema = (clickhouseConfig, tableName) => {
  return axios
    .post(
      `${URL}/clickhouse/schema?tableName=${encodeURIComponent(tableName)}`,
      clickhouseConfig
    )
    .then((res) => res.data)
    .catch((err) => {
      throw err;
    });
};

export {
  testClickhouseConnection,
  fetchClickhouseTables,
  fetchClickhouseSchema,
};
