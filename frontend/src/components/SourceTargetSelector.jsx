import { useState } from "react";

const SourceTargetSelector = ({ onSourceChange, onTargetChange }) => {
  const [selectedDirection, setSelectedDirection] = useState("");

  const handleSelection = (source, target, directionKey) => {
    setSelectedDirection(directionKey);
    onSourceChange(source);
    onTargetChange(target);
  };

  return (
    <div className="bg-white shadow rounded-lg mb-6 border border-gray-300">
      <div className="bg-blue-500 text-white px-6 py-3 rounded-t-lg">
        <h5 className="text-lg font-medium">
          Step 1: Select Source and Target
        </h5>
      </div>

      <div className="px-6 py-4">
        <p className="text-md font-mono text-gray-700 mb-3">
          Choose the direction of data flow:
        </p>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* ClickHouse → FlatFile */}
          <button
            onClick={() =>
              handleSelection("clickhouse", "flatfile", "clickhouse-flatfile")
            }
            className={`w-full text-left border rounded px-4 py-3 transition ${
              selectedDirection === "clickhouse-flatfile"
                ? "border-cyan-500 shadow"
                : "border-gray-300 hover:border-blue-400 hover:shadow"
            }`}
          >
            <span className="font-semibold text-gray-800">ClickHouse</span>
            <span className="mx-2 text-gray-500">→</span>
            <span className="font-semibold text-gray-800">Flat File</span>
            <p className="text-sm text-gray-500 mt-1">
              Export data from ClickHouse into a flat file.
            </p>
          </button>

          {/* FlatFile → ClickHouse */}
          <button
            onClick={() =>
              handleSelection("flatfile", "clickhouse", "flatfile-clickhouse")
            }
            className={`w-full text-left border rounded px-4 py-3 transition ${
              selectedDirection === "flatfile-clickhouse"
                ? "border-cyan-500 shadow"
                : "border-gray-300 hover:border-blue-400 hover:shadow"
            }`}
          >
            <span className="font-semibold text-gray-800">Flat File</span>
            <span className="mx-2 text-gray-500">→</span>
            <span className="font-semibold text-gray-800">ClickHouse</span>
            <p className="text-sm text-gray-500 mt-1">
              Ingest flat file data into ClickHouse.
            </p>
          </button>
        </div>
      </div>
    </div>
  );
};

export default SourceTargetSelector;
