import { useEffect, useRef, useState } from "react";

import { Typography } from "@mui/material";

import { http_endpoint } from "../../config";

type Timing = {
  state: "paused" | "waiting" | "running";
  remainingMs: number;
};

export default function RoundCountdown() {
  const [state, setState] = useState<Timing["state"] | "unavailable">(
    "unavailable"
  );
  const [seconds, setSeconds] = useState<number>();
  const deadline = useRef(0);

  useEffect(() => {
    const controller = new AbortController();
    const updateSeconds = () => {
      setSeconds(Math.max(0, Math.ceil((deadline.current - performance.now()) / 1000)));
    };
    const refresh = async () => {
      try {
        const response = await fetch(`${http_endpoint}/api/engine/timing`, {
          credentials: "include",
          signal: controller.signal,
        });
        if (!response.ok) throw new Error(`Timing request failed: ${response.status}`);
        const timing: Timing = await response.json();
        if (controller.signal.aborted) return;
        deadline.current = performance.now() + timing.remainingMs;
        setState(timing.state);
        updateSeconds();
      } catch {
        if (!controller.signal.aborted) setState("unavailable");
      }
    };

    void refresh();
    const refreshTimer = window.setInterval(refresh, 3000);
    const displayTimer = window.setInterval(updateSeconds, 250);
    document.addEventListener("visibilitychange", refresh);
    return () => {
      controller.abort();
      window.clearInterval(refreshTimer);
      window.clearInterval(displayTimer);
      document.removeEventListener("visibilitychange", refresh);
    };
  }, []);

  let label = "Countdown unavailable";
  if (state === "paused") label = "Engine paused";
  if (state === "waiting" || state === "running") {
    label = seconds === 0 ? "Next round starting…" : `Next round in ${seconds ?? "--"}s`;
  }

  return (
    <Typography variant='h6' color='text.secondary' sx={{ mt: 0.5 }}>
      {label}
    </Typography>
  );
}
