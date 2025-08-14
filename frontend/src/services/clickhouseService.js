import axios from "axios";

const URL = import.meta.env.VITE_BACKEND_BASE_URL;

const testClickhouseConnection = (clickhouseConfig) => {
  return axios
    .post(`${URL}/clickhouse/test-connection`, clickhouseConfig)
    .then((res) => res.data)
    .catch((err) => {
      throw err;
    });
};

const fetchClickhouseTables = (clickhouseConfig) => {
  return axios
    .post(`${URL}/clickhouse/tables`, clickhouseConfig)
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

const fetchClickhousePreviewData = (ingestionReq) => {
  return axios
    .post(`${URL}/clickhouse/preview`, ingestionReq)
    .then((res) => res.data)
    .catch((err) => {
      throw err;
    });
};

export {
  testClickhouseConnection,
  fetchClickhouseTables,
  fetchClickhouseSchema,
  fetchClickhousePreviewData,
};
