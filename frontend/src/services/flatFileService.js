import axios from "axios";

const URL = "http://localhost:8080/api/integration";

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


export { fetchFlatFileSchema, fetchFlatFilePreviewData };
