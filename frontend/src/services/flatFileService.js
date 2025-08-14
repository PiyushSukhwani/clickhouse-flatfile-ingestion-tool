import axios from "axios";

const URL = import.meta.env.VITE_BACKEND_BASE_URL;

const fetchFlatFileSchema = (formDataRef) => {
  const formData = new FormData();

  formData.append(
    "flatFileConfig",
    new Blob([formDataRef.current.get("flatFileConfig") || "{}"], {
      type: "application/json",
    })
  );

  // If user uploaded file, append it
  if (formDataRef.current.get("file")) {
    formData.append("file", formDataRef.current.get("file"));
  }

  return axios
    .post(`${URL}/flatfile/schema`, formData, {
      headers: {
        "Content-Type": "multipart/form-data",
      },
    })
    .then((res) => res.data)
    .catch((err) => {
      throw err;
    });
};

const fetchFlatFilePreviewData = (formDataRef) => {
  const formData = new FormData();

  const ingestionRequest = {
    flatFileConfig: JSON.parse(
      formDataRef.current.get("flatFileConfig") || "{}"
    ),
    selectedColumns: JSON.parse(
      formDataRef.current.get("selectedColumns") || "[]"
    ),
  };

  formData.append(
    "ingestionRequest",
    new Blob([JSON.stringify(ingestionRequest)], { type: "application/json" })
  );

  // If user uploaded file, append it
  if (formDataRef.current.get("file")) {
    formData.append("file", formDataRef.current.get("file"));
  }

  return axios
    .post(`${URL}/flatfile/preview`, formData, {
      headers: {
        "Content-Type": "multipart/form-data",
      },
    })
    .then((res) => res.data)
    .catch((err) => {
      throw err;
    });
};

const executeIngestion = (formDataRef) => {
  const formData = new FormData();

  const ingestionRequest = {
    sourceType: formDataRef.current.get("sourceType") || "",
    targetType: formDataRef.current.get("targetType") || "",
    clickHouseConfig: JSON.parse(
      formDataRef.current.get("clickHouseConfig") || "{}"
    ),
    flatFileConfig: JSON.parse(
      formDataRef.current.get("flatFileConfig") || "{}"
    ),
    tableName: formDataRef.current.get("tableName") || "",
    additionalTables: JSON.parse(
      formDataRef.current.get("additionalTables") || "[]"
    ),
    joinCondition: formDataRef.current.get("joinCondition") || "",
    selectedColumns: JSON.parse(
      formDataRef.current.get("selectedColumns") || "[]"
    ),
    targetTableName: formDataRef.current.get("targetTableName") || "",
  };

  formData.append(
    "ingestionRequest",
    new Blob([JSON.stringify(ingestionRequest)], { type: "application/json" })
  );

  if (formDataRef.current.get("file")) {
    formData.append("file", formDataRef.current.get("file"));
  }

  return axios
    .post(`${URL}/execute`, formData, {
      headers: {
        "Content-Type": "multipart/form-data",
      },
      responseType:
        ingestionRequest.targetType === "flatfile" ? "blob" : "json",
    })
    .then((res) => {
      if (ingestionRequest.targetType === "flatfile") {
        // Read recordCount from header (server must send header as X-Record-Count)
        const recordCount = res.headers["x-record-count"];

        // Automatically download
        const url = window.URL.createObjectURL(res.data);
        const link = document.createElement("a");
        link.href = url;
        link.setAttribute(
          "download",
          `${ingestionRequest.tableName || "data"}.csv`
        );
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);

        return Number(recordCount);
      } else {
        return res.data;
      }
    })
    .catch((err) => {
      throw err;
    });
};

export { fetchFlatFileSchema, fetchFlatFilePreviewData, executeIngestion };
