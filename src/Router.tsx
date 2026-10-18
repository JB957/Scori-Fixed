import { ReactElement, ReactNode, Suspense, createContext, lazy, useContext } from "react";
import { RouterProvider, createBrowserRouter } from "react-router-dom";

import { ApolloClient, NormalizedCacheObject } from "@apollo/client";
import { CircularProgress } from "@mui/material";
import { enqueueSnackbar } from "notistack";

import { Admin, Authenticated, Error, Main, User } from "./components";
import { AuthContext } from "./components/Context/AuthProvider";
import {
  EngineState,
  useEngineStateSubscription,
  useGlobalNotificationSubscription,
} from "./graph";
import { useAuth } from "./hooks";
const AdminChecks = lazy(() => import("./pages/Admin/Checks"));
const AdminKoth = lazy(() => import("./pages/Admin/Koth"));
const Minions = lazy(() => import("./pages/Admin/Minions"));
const AdminPanel = lazy(() => import("./pages/Admin/AdminPanel"));
const ChangePassword = lazy(() => import("./pages/ChangePassword"));
const Home = lazy(() => import("./pages/Home"));
const Injects = lazy(() => import("./pages/Admin/Injects"));
const Login = lazy(() => import("./pages/Login"));
const Me = lazy(() => import("./pages/Me"));
const Scoreboard = lazy(() => import("./pages/Scoreboard/Scoreboard"));
const ScoreboardRound = lazy(() => import("./pages/Scoreboard/Round"));
const UserChecks = lazy(() => import("./pages/User/Checks"));
const UserInjects = lazy(() => import("./pages/User/Injects"));
const Users = lazy(() => import("./pages/Admin/Users"));
const KothScoreboard = lazy(() => import("./pages/Scoreboard/KothScoreboard"));
const KothScoreboardRound = lazy(() => import("./pages/Scoreboard/KothRound"));
const Statuses = lazy(() => import("./pages/Statuses"));

const LazyComponent = ({ element }: { element: ReactNode }): ReactElement => {
  return <Suspense fallback={<CircularProgress />}>{element}</Suspense>;
};

type props = {
  theme: "dark" | "light";
  setTheme: React.Dispatch<React.SetStateAction<"dark" | "light">>;
  apolloClient: ApolloClient<NormalizedCacheObject>;
};

type ShellState = Pick<props, "theme" | "setTheme"> & { engineState?: EngineState };
const ShellContext = createContext<ShellState | null>(null);

function useShell(): ShellState {
  const shell = useContext(ShellContext);
  if (!shell) throw new globalThis.Error("Router shell is missing");
  return shell;
}

function MainRoute() {
  const { theme, setTheme, engineState } = useShell();
  return <Main theme={theme} setTheme={setTheme} engineState={engineState} />;
}

function ScoreboardRoute() {
  const { theme } = useShell();
  return <Scoreboard theme={theme} />;
}

function ScoreboardRoundRoute() {
  const { theme } = useShell();
  return <ScoreboardRound theme={theme} />;
}

function KothScoreboardRoute() {
  const { theme } = useShell();
  return <KothScoreboard theme={theme} />;
}

function KothScoreboardRoundRoute() {
  const { theme } = useShell();
  return <KothScoreboardRound theme={theme} />;
}

function AdminPanelRoute() {
  const { engineState } = useShell();
  return <AdminPanel engineState={engineState} />;
}

const router = createBrowserRouter([
    {
      path: "/",
      element: (
        <LazyComponent
          element={
            <MainRoute />
          }
        />
      ),
      children: [
        {
          index: true,
          element: <LazyComponent element={<Home />} />,
        },
        {
          path: "login",
          element: <LazyComponent element={<Login />} />,
        },
        {
          element: <LazyComponent element={<Authenticated />} />,
          children: [
            {
              path: "me",
              element: <LazyComponent element={<Me />} />,
            },
            {
              path: "password",
              element: <LazyComponent element={<ChangePassword />} />,
            },
            {
              path: "status",
              element: <LazyComponent element={<Statuses />} />,
            },
          ],
        },
        {
          path: "scoreboard",
          children: [
            {
              index: true,
              element: <LazyComponent element={<ScoreboardRoute />} />,
            },
            {
              path: ":round",
              element: (
                <LazyComponent element={<ScoreboardRoundRoute />} />
              ),
            },
          ],
        },
        {
          path: "koth-scoreboard",
          children: [
            {
              index: true,
              element: (
                <LazyComponent element={<KothScoreboardRoute />} />
              ),
            },
            {
              path: ":round",
              element: (
                <LazyComponent
                  element={<KothScoreboardRoundRoute />}
                />
              ),
            },
          ],
        },
        {
          path: "admin",
          element: <LazyComponent element={<Admin />} />,
          children: [
            {
              index: true,
              element: (
                <LazyComponent
                  element={
                    <AdminPanelRoute />
                  }
                />
              ),
            },
            {
              path: "checks",
              element: <LazyComponent element={<AdminChecks />} />,
            },
            {
              path: "users",
              element: <LazyComponent element={<Users />} />,
            },
            {
              path: "injects",
              element: <LazyComponent element={<Injects />} />,
            },
            {
              path: "minions",
              element: <LazyComponent element={<Minions />} />,
            },
            {
              path: "koth",
              element: <LazyComponent element={<AdminKoth />} />,
            },
          ],
        },
        {
          element: <LazyComponent element={<User />} />,
          children: [
            {
              path: "checks",
              element: <LazyComponent element={<UserChecks />} />,
            },
            {
              path: "injects",
              element: <LazyComponent element={<UserInjects />} />,
            },
          ],
        },
        {
          path: "*",
          element: (
            <LazyComponent
              element={<Error code={404} message={"Page Not Found"} />}
            />
          ),
        },
      ],
    },
  ]);

export function Router({ theme, setTheme, apolloClient }: props) {
  const {
    jwt,
    me,
    meLoading,
    meError,
    cookies,
    setCookie,
    removeCookie,
    updateCookie,
  } = useAuth(apolloClient);

  useGlobalNotificationSubscription({
    onData: (data) => {
      if (data.data.data?.globalNotification) {
        enqueueSnackbar(data.data.data.globalNotification.message, {
          variant: data.data.data.globalNotification.type,
        });
      }
    },
    onError: (error) => {
      console.error(error);
    },
  });

  const { data: engineState } = useEngineStateSubscription({
    onError: (error) => {
      console.error(error);
    },
  });



  return (
    <AuthContext.Provider
      value={{
        jwt,
        cookies,
        setCookie,
        removeCookie,
        updateCookie,
        me,
        meLoading,
        meError,
      }}
    >
      <ShellContext.Provider value={{ theme, setTheme, engineState: engineState?.engineState }}>
        <RouterProvider router={router} />
      </ShellContext.Provider>
    </AuthContext.Provider>
  );
}
