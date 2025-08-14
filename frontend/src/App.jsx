import { useEffect, useRef, useState } from "react";
import SourceTargetSelector from "./components/SourceTargetSelector";
import ColumnSelectionSection from "./components/ColumnSelect";
import DataPreview from "./components/PreviewData";
import IngestionSection from "./components/IngestionSection";
import ConfigRenderer from "./components/ConfigRenderer";

function App() {
  const [source, setSource] = useState("");
  const [target, setTarget] = useState("");
  const [columns, setColumns] = useState([]);
  const [previewData, setPreviewData] = useState([]);
  const [previewDataMessage, setPreviewDataMessage] = useState("");
  const formDataRef = useRef(new FormData());

  useEffect(() => {
    setColumns([]);
    setPreviewData([]);
    setPreviewDataMessage("");
    formDataRef.current.delete("clickHouseConfig");
    formDataRef.current.delete("flatFileConfig");
    formDataRef.current.delete("selectedColumns");
  }, [source, target]);

  return (
    <div className="container mx-auto mt-6 px-4">
      <h1 className="text-4xl text-center font-semibold text-gray-800 mb-6">
        ClickHouse & Flat File Data Ingestion Tool
      </h1>
      <div className="max-w-7xl mx-auto mt-10">
        <SourceTargetSelector
          formDataRef={formDataRef}
          onSourceChange={setSource}
          onTargetChange={setTarget}
        />

        <ConfigRenderer
          type={source}
          setColumns={setColumns}
          formDataRef={formDataRef}
        />
        <ConfigRenderer
          type={target}
          selectionType="target"
          formDataRef={formDataRef}
        />

        {columns?.length > 0 && (
          <ColumnSelectionSection
            source={source}
            columns={columns}
            formDataRef={formDataRef}
            setPreviewData={setPreviewData}
            setPreviewDataMessage={setPreviewDataMessage}
          />
        )}

        <DataPreview
          data={previewData}
          visible={previewData.length > 0 || previewDataMessage}
          formDataRef={formDataRef}
          previewDataMessage={previewDataMessage}
        />

        <IngestionSection
          source={source}
          visible={
            formDataRef.current.get("clickHouseConfig") &&
            formDataRef.current.get("flatFileConfig") &&
            formDataRef.current.get("selectedColumns") &&
            previewData?.length > 0
          }
          formDataRef={formDataRef}
        />
      </div>
    </div>
  );
}

export default App;
