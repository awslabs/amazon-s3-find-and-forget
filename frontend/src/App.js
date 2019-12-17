import React, { useState } from "react";
import { Authenticator, Greetings, SignUp } from "aws-amplify-react";

import AppLayout from "./components/AppLayout";
import ConfigurationHelp from "./components/help/Configuration";
import ConfigurationPage from "./components/pages/Configuration";
import DashboardHelp from "./components/help/Dashboard";
import DashboardPage from "./components/pages/Dashboard";
import DeletionQueueHelp from "./components/help/DeletionQueue";
import DeletionQueuePage from "./components/pages/DeletionQueue";
import Header from "./components/Header";
import DeletionJobsHelp from "./components/help/DeletionJobs";
import DeletionJobsPage from "./components/pages/DeletionJobs";

import gateway from "./utils/gateway";

export default () => {
  const [authState, setAuthState] = useState(undefined);
  const [currentPage, setCurrentPage] = useState(0);

  const pages = [
    {
      title: "Dashboard",
      page: <DashboardPage onStartDeletionJobClick={() => setCurrentPage(3)} />,
      help: <DashboardHelp />
    },
    {
      title: "Configuration",
      page: <ConfigurationPage />,
      help: <ConfigurationHelp />
    },
    {
      title: "Deletion Queue",
      page: <DeletionQueuePage />,
      help: <DeletionQueueHelp />
    },
    {
      title: "Deletion Jobs",
      page: <DeletionJobsPage />,
      help: <DeletionJobsHelp />
    }
  ];

  const classNames = ["App"];
  const signedIn = authState === "signedIn";
  if (!signedIn) classNames.push("amplify-auth");

  return (
    <div className={classNames.join(" ")}>
      <Header signedIn={signedIn} />
      <Authenticator
        onStateChange={s => setAuthState(s)}
        hide={[Greetings, SignUp]}
      >
        {signedIn && (
          <AppLayout
            currentPage={currentPage}
            onMenuClick={setCurrentPage}
            pages={pages}
          />
        )}
      </Authenticator>
    </div>
  );
};
