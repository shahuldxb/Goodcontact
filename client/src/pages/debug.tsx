import DirectTestResults from "@/components/direct-test-results";

export default function DebugPage() {
  return (
    <div className="w-full py-4">
      <h1 className="text-3xl font-bold mb-8 px-4">Debug Tools</h1>
      <DirectTestResults />
    </div>
  );
}