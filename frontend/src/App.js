import React, { useEffect, useState } from "react";
import { AmplifyAuthenticator, AmplifySignIn } from "@aws-amplify/ui-react";

import AppLayout from "./components/AppLayout";
import Header from "./components/Header";
import {
  DashboardHelp,
  DataMappersHelp,
  DeletionJobDetailsHelp,
  DeletionJobsHelp,
  DeletionQueueHelp,
  NewDataMapperHelp,
  NewDeletionQueueMatchHelp,
} from "./components/help";
import {
  DashboardPage,
  DataMappersPage,
  DeletionJobPage,
  DeletionJobsPage,
  DeletionQueuePage,
  NewDataMapperPage,
  NewDeletionQueueMatchPage,
} from "./components/pages";
import gateway from "./utils/gateway";
import { isUndefined } from "./utils";
import { Auth } from "aws-amplify";

export default () => {
  const [authState, setAuthState] = useState(undefined);
  const [currentPage, setCurrentPage] = useState(0);
  const [selectedJobId, selectJobId] = useState(undefined);

  const goToJobDetails = (jobId) => {
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
      help: <DashboardHelp />,
    },
    {
      title: "Data Mappers",
      page: <DataMappersPage gateway={gateway} onPageChange={setCurrentPage} />,
      help: <DataMappersHelp />,
    },
    {
      title: "Deletion Queue",
      page: (
        <DeletionQueuePage gateway={gateway} onPageChange={setCurrentPage} />
      ),
      help: <DeletionQueueHelp />,
    },
    {
      title: "Deletion Jobs",
      page: (
        <DeletionJobsPage gateway={gateway} goToJobDetails={goToJobDetails} />
      ),
      help: <DeletionJobsHelp />,
    },
    {
      title: "Create Data Mapper",
      page: (
        <NewDataMapperPage
          gateway={gateway}
          goToDataMappers={() => setCurrentPage(1)}
        />
      ),
      help: <NewDataMapperHelp />,
      parent: 1,
    },
    {
      title: "Add item to the Deletion Queue",
      page: (
        <NewDeletionQueueMatchPage
          gateway={gateway}
          goToDeletionQueue={() => setCurrentPage(2)}
        />
      ),
      help: <NewDeletionQueueMatchHelp />,
      parent: 2,
    },
    {
      title: selectedJobId || "Deletion Job details",
      page: (
        <DeletionJobPage
          gateway={gateway}
          goToJobsList={() => setCurrentPage(3)}
          jobId={selectedJobId}
        />
      ),
      help: <DeletionJobDetailsHelp />,
      parent: 3,
    },
  ];

  useEffect(() => {
    if (isUndefined(authState)) {
      // We want to establish if authState is undefined because the user is not authenticated
      // or because the state hasn't been updated yet by the authenticator. This is important
      // because the <AmplifyAuthenticator> doesn't show if the user is already authenticated,
      // which is a scenario that can happen if the user closes the page and we want to persist
      // its session.
      Auth.currentAuthenticatedUser()
        .then(() => setAuthState("signedin"))
        .catch(() => {
          // The user appears unauthenticated. All fine. The <AmplifyAuthenticator> will be
          // rendered correctly.
        });
    }
  }, [authState]);

  const signedIn = authState === "signedin";
  return (
    <div className="App">
      <Header signedIn={signedIn} />
      {signedIn ? (
        <AppLayout
          currentPage={currentPage}
          onMenuClick={setCurrentPage}
          pages={pages}
        />
      ) : (
        <div className="amplify-auth-container">
          <AmplifyAuthenticator usernameAlias="email">
            <AmplifySignIn
              slot="sign-in"
              usernameAlias="email"
              handleAuthStateChange={(s) => setAuthState(s)}
              formFields={[
                {
                  type: "email",
                  label: "Username *",
                  placeholder: "Enter your username",
                  required: true,
                  inputProps: { autoComplete: "off" },
                },
                {
                  type: "password",
                  label: "Password *",
                  placeholder: "Enter your password",
                  required: true,
                  inputProps: { autoComplete: "off" },
                },
              ]}
            >
              <div slot="secondary-footer-content"></div>
            </AmplifySignIn>
          </AmplifyAuthenticator>
        </div>
      )}
    </div>
  );
};
