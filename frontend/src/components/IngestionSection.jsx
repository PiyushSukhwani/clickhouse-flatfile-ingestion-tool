import { useRef, useState } from "react";
import { executeIngestion } from "../services/flatFileService";

const IngestionSection = ({ source, visible, formDataRef }) => {
  const [showProgress, setShowProgress] = useState(false);
  const [progress, setProgress] = useState(0);
  const [showStatus, setShowStatus] = useState(false);
  const [statusMessage, setStatusMessage] = useState("");
  const [statusType, setStatusType] = useState("success");
  const [showResult, setShowResult] = useState(false);
  const [totalRecords, setTotalRecords] = useState(0);

  const ref = useRef(null);

  const onStart = async () => {
    setShowProgress(true);
    setShowStatus(false);
    setProgress(0);
    setShowResult(false);

    try {
      const res = await executeIngestion(formDataRef);

      const interval = setInterval(() => {
        setProgress((prev) => {
          if (prev >= 100) {
            clearInterval(interval);
            setShowProgress(false);
            setShowStatus(true);
            setStatusType("success");
            setStatusMessage(
              source === "clickhouse"
                ? "Export completed successfully. Your updated flatfile has been downloaded."
                : "Import completed successfully. All records have been added to your ClickHouse table."
            );
            setShowResult(true);

            // Scroll to bottom
            setTimeout(() => {
              ref.current?.scrollIntoView({ behavior: "smooth" });
            }, 100);

            return 100;
          }
          return prev + 30;
        });
      }, 300);

      setTotalRecords(res); // record count
    } catch (error) {
      setShowProgress(false);
      setShowStatus(true);
      setStatusType("error");
      setStatusMessage(error.response.data);
    }
  };

  return (
    <div
      className={`${
        visible ? "block" : "hidden"
      } mb-6 rounded-lg shadow border-gray-400 border`}
    >
      <div className="bg-blue-500  text-white px-4 py-3 rounded-t-lg">
        <h5 className="text-lg font-semibold">Step 6: Execute Ingestion</h5>
      </div>
      <div className="p-4">
        <div className="mb-4">
          <button
            onClick={onStart}
            className="bg-green-600 hover:bg-green-700 text-white font-medium px-5 py-2 rounded flex justify-center items-center gap-2"
          >
            {showProgress ? (
              <>
                <div className="h-4 w-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                {source === "clickhouse"
                  ? "Exporting, please wait..."
                  : "Importing, please wait..."}
              </>
            ) : (
              "Start Ingestion"
            )}
          </button>
        </div>

        {showProgress && (
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-1">
              {source === "clickhouse"
                ? "Exporting data from ClickHouse and preparing your flatfile..."
                : "Importing data from flatfile into ClickHouse..."}
            </label>
            <div className="w-full bg-gray-200 rounded h-6 overflow-hidden">
              <div
                className="bg-green-500 h-full transition-all duration-300 ease-in-out"
                style={{ width: `${progress}%` }}
              />
            </div>
          </div>
        )}

        {showStatus && (
          <div className="mb-4" ref={ref}>
            <div
              className={`rounded px-4 py-3 text-sm font-medium ${
                statusType === "success"
                  ? "bg-green-100 text-green-800 border border-green-300"
                  : statusType === "error"
                  ? "bg-red-100 text-red-800 border border-red-300"
                  : "bg-yellow-100 text-yellow-800 border border-yellow-300"
              }`}
            >
              {statusMessage}
            </div>
          </div>
        )}

        {showResult && (
          <div className="mt-4">
            <div className="bg-green-100 border border-green-300 text-green-800 rounded px-4 py-3">
              <h5 className="font-semibold text-lg">Ingestion Completed</h5>
              <p className="mt-1">
                Total records processed:{" "}
                <span className="font-bold">{totalRecords}</span>
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default IngestionSection;
