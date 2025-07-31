import React from "react";

const IngestionSection = ({
  visible,
  onStart,
  showProgress,
  progress,
  showStatus,
  statusMessage,
  statusType, // e.g., "success", "error", "warning"
  showResult,
  totalRecords,
  ref
}) => {
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
            className="bg-green-600 hover:bg-green-700 text-white font-medium px-5 py-2 rounded"
          >
            Start Ingestion
          </button>
        </div>

        {showProgress && (
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Ingestion Progress:
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
