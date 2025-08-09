import React, { useEffect, useState } from "react";
import { fetchClickhousePreviewData } from "../services/clickhouseService";
import { fetchFlatFilePreviewData } from "../services/flatFileService";

const ColumnSelectionSection = ({
  columns,
  formDataRef,
  setPreviewData,
  source,
  setPreviewDataMessage,
}) => {
  const [columnsState, setColumnsState] = useState([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const initialized = columns.map((col) => ({ ...col, selected: true }));
    setColumnsState(initialized);
    formDataRef.current.set("selectedColumns", JSON.stringify(initialized));
  }, [columns]);

  const handleSelectAll = () => {
    const updated = columnsState.map((col) => ({ ...col, selected: true }));
    setColumnsState(updated);
    formDataRef.current.set("selectedColumns", JSON.stringify(updated));
  };

  const handleDeselectAll = () => {
    const updated = columnsState.map((col) => ({ ...col, selected: false }));
    setColumnsState(updated);
    formDataRef.current.set("selectedColumns", JSON.stringify(updated));
  };

  const handleCheckboxChange = (idx) => {
    const updated = [...columnsState];
    updated[idx].selected = !updated[idx].selected;
    setColumnsState(updated);
    formDataRef.current.set("selectedColumns", JSON.stringify(updated));
  };

  const handleFetchData = async () => {
    setLoading(true);
    setPreviewDataMessage("");

    try {
      let res;

      if (source === "clickhouse") {
        const fd = formDataRef.current;
        const previewRequest = {
          clickHouseConfig: JSON.parse(fd.get("clickhouseConfig") || "{}"),
          tableName: fd.get("tableName") || "",
          selectedColumns: columnsState,
        };

        res = await fetchClickhousePreviewData(previewRequest);
      } else {
        res = await fetchFlatFilePreviewData(formDataRef);
      }

      setPreviewData(res.data);
      res.data.length > 0
        ? setPreviewDataMessage("")
        : setPreviewDataMessage("No data available for preview");
    } catch (error) {
      setPreviewDataMessage(error.response.data.message);
      console.error(error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="mb-4 shadow">
      <div className="bg-blue-500 text-white px-4 py-3 rounded-t">
        <h5 className="mb-0 text-lg font-semibold">
          Step 4: Select Columns for Ingestion
        </h5>
      </div>

      <div className="bg-white p-4 rounded-b shadow">
        <div className="mb-3">
          <div className="flex justify-between items-center mb-2">
            <h6 className="text-base font-medium">Available Columns</h6>
            <div className="space-x-2">
              <button
                onClick={handleSelectAll}
                className="btn-sm px-3 py-1 text-sm border border-blue-500 text-blue-600 hover:bg-blue-600 hover:text-white rounded"
              >
                Select All
              </button>
              <button
                onClick={handleDeselectAll}
                className="btn-sm px-3 py-1 text-sm border border-gray-400 text-gray-700 hover:bg-gray-600 hover:text-white rounded"
              >
                Deselect All
              </button>
            </div>
          </div>

          <div className="border border-gray-300 p-3 rounded overflow-y-auto max-h-72 space-y-1">
            {columnsState?.map((col, idx) => (
              <div key={idx} className="flex items-center space-x-2">
                <input
                  checked={col.selected}
                  onChange={() => handleCheckboxChange(idx)}
                  type="checkbox"
                  id={`col-${idx}`}
                  className="w-4 h-4 rounded focus:ring-2 focus:ring-[rgba(70,130,246,0.6)] focus:outline-none"
                />
                <label
                  htmlFor={`col-${idx}`}
                  className="text-md font-serif text-gray-700"
                >
                  {col.name}
                </label>
              </div>
            ))}
          </div>
        </div>

        <div>
          <button
            onClick={handleFetchData}
            disabled={loading}
            className={`w-full md:w-auto px-4 py-2 rounded text-white flex items-center justify-center gap-2
    ${
      loading
        ? "bg-blue-400 cursor-not-allowed"
        : "bg-blue-600 hover:bg-blue-700"
    }
  `}
          >
            {loading ? (
              <>
                <div className="h-4 w-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                Fetching...
              </>
            ) : (
              "Preview Data"
            )}
          </button>
        </div>
      </div>
    </div>
  );
};

export default ColumnSelectionSection;
