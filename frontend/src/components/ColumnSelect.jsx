import React from "react";

const ColumnSelectionSection = ({
  isVisible,
  onSelectAll,
  onDeselectAll,
  onPreviewData,
  columns,
}) => {
  return (
    <div className={` mb-4 shadow ${!isVisible ? "hidden" : ""}`}>
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
                onClick={onSelectAll}
                className="btn-sm px-3 py-1 text-sm border border-blue-500 text-blue-600 hover:bg-blue-600 hover:text-white rounded"
              >
                Select All
              </button>
              <button
                onClick={onDeselectAll}
                className="btn-sm px-3 py-1 text-sm border border-gray-400 text-gray-700 hover:bg-gray-600 hover:text-white rounded"
              >
                Deselect All
              </button>
            </div>
          </div>

          <div className="border border-gray-300 p-3 rounded overflow-y-auto max-h-72 space-y-1">
            {columns?.map((col, idx) => (
              <div key={idx} className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  id={`col-${idx}`}
                  className="w-4 h-4 rounded focus:ring-2 focus:ring-[rgba(70,130,246,0.6)] focus:outline-none"
                />
                <label htmlFor={`col-${idx}`} className="text-md font-serif text-gray-700">
                  {col}
                </label>
              </div>
            ))}
          </div>
        </div>

        <div>
          <button
            onClick={onPreviewData}
            className="bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-cyan-700 transition"
          >
            Preview Data
          </button>
        </div>
      </div>
    </div>
  );
};

export default ColumnSelectionSection;
