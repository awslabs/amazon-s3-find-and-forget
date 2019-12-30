import React, { useState } from "react";
import { Authenticator, Greetings, SignUp } from "aws-amplify-react";

import AppLayout from "./components/AppLayout";
import ConfigurationHelp from "./components/help/Configuration";
import ConfigurationPage from "./components/pages/Configuration";
import DashboardHelp from "./components/help/Dashboard";
import DashboardPage from "./components/pages/Dashboard";
import DeletionJob from "./components/pages/DeletionJob";
import DeletionJobsHelp from "./components/help/DeletionJobs";
import DeletionJobsPage from "./components/pages/DeletionJobs";
import DeletionQueueHelp from "./components/help/DeletionQueue";
import DeletionQueuePage from "./components/pages/DeletionQueue";
import Header from "./components/Header";
import NewConfiguration from "./components/pages/NewConfiguration";
import NewDeletionQueueMatch from "./components/pages/NewDeletionQueueMatch";

import gateway from "./utils/gateway";

export default () => {
  const [authState, setAuthState] = useState(undefined);
  const [currentPage, setCurrentPage] = useState(0);
  const [selectedJobId, selectJobId] = useState(undefined);

  const goToJobDetails = jobId => {
    selectJobId(jobId);
    setCurrentPage(6);
  };

  const pages = [
    {
      title: "Dashboard",
      page: (
        <DashboardPage
          gateway={gateway}
          goToJobDetails={goToJobDetails}
          goToPage={setCurrentPage}
        />
      ),
      help: <DashboardHelp />
    },
    {
      title: "Configuration",
      page: (
        <ConfigurationPage gateway={gateway} onPageChange={setCurrentPage} />
      ),
      help: <ConfigurationHelp />
    },
    {
      title: "Deletion Queue",
      page: (
        <DeletionQueuePage gateway={gateway} onPageChange={setCurrentPage} />
      ),
      help: <DeletionQueueHelp />
    },
    {
      title: "Deletion Jobs",
      page: (
        <DeletionJobsPage gateway={gateway} goToJobDetails={goToJobDetails} />
      ),
      help: <DeletionJobsHelp />
    },
    {
      title: "Create Data Mapper",
      page: (
        <NewConfiguration
          gateway={gateway}
          goToDataMappers={() => setCurrentPage(1)}
        />
      ),
      parent: 1
    },
    {
      title: "Add item to the Deletion Queue",
      page: (
        <NewDeletionQueueMatch
          gateway={gateway}
          goToDeletionQueue={() => setCurrentPage(2)}
        />
      ),
      parent: 2
    },
    {
      title: selectedJobId || "Deletion Job details",
      page: (
        <DeletionJob
          gateway={gateway}
          goToJobsList={() => setCurrentPage(3)}
          jobId={selectedJobId}
        />
      ),
      parent: 3
    }
  ];

  const classNames = ["App"];
  const signedIn = authState === "signedIn";
  if (!signedIn) classNames.push("amplify-auth");

  return (
    <div className={classNames.join(" ")}>
      <Header signedIn={signedIn} />
      {signedIn ? (
        <AppLayout
          currentPage={currentPage}
          onMenuClick={setCurrentPage}
          pages={pages}
        />
      ) : (
        <Authenticator
          onStateChange={s => setAuthState(s)}
          hide={[Greetings, SignUp]}
        />
      )}
    </div>
  );
};
