#ifndef H_POPEN
#define H_POPEN

// from https://stackoverflow.com/a/45308042

#include <iostream>
#include <sstream>
#include <string>
#include <cstring>   // sterror
#include <cstdio>  // popen, FILE, fgets, fputs
#include <cassert>


class POpen_t    // access to ::popen
{
protected:
   FILE*        m_FILE;
   std::string  m_cmd;

public:
   POpen_t(void) : m_FILE(nullptr)
      { }

   virtual ~POpen_t(void) {
      if (m_FILE) (void)close();
      m_FILE = 0;
   }

   // on success: 0 == return.size(), else returns error msg
   std::string  close()
      {
         std::stringstream errSS;

         do // poor man's try block
         {
            // pclose() returns the term status of the shell cmd
            // otherwise -1 and sets errno.
            assert(nullptr != m_FILE);  // tbr - some sort of logic error
            // tbr if(0 == m_FILE)  break;  // success?

            int32_t pcloseStat = ::pclose(m_FILE);
            int myErrno = errno;
            if (0 != pcloseStat)
            {
               errSS << "\n  POpen_t::close() errno " << myErrno
                     << "  " << std::strerror(myErrno) << std::endl;
               break;
            }

            m_FILE     = 0;

         }while(0);

         return(errSS.str());
      } // std::string  close(void)

}; // class POpen_t

class POpenRead_t : public POpen_t    // access to ::popen read-mode
{

public:
   POpenRead_t(void) { }

   // ::popen(aCmd): opens a process (fork), invokes shell, 
   //                and creates a pipe
   // returns NULL if the fork or pipe calls fail,
   //              or if it cannot allocate memory.
   // on success: 0 == return.size(), else returns error msg
   std::string  open (std::string aCmd)
      {
         std::stringstream errSS; // 0 == errSS.str().size() is success
         assert (aCmd.size() > 0);
         assert (0 == m_FILE); // can only use serially
         m_cmd = aCmd; // capture

         do  // poor man's try block
         {
            if(true) // diagnosis only
               std::cout << "\n  POpenRead_t::open(cmd): cmd: '"
                         << m_cmd << "'\n" << std::endl;

            // ::popen(aCmd): opens a process by creating a pipe, forking,
            //                and invoking the shell.
            // returns NULL if the fork or pipe calls fail,
            //              or if it cannot allocate memory.
            m_FILE = ::popen (m_cmd.c_str(), "r"); // create 'c-stream' (FILE*)
            //       ^^ function is not in namespace std::

            int myErrno = errno;
            if(0 == m_FILE)
            {
               errSS << "\n  POpenRead_t::open(" << m_cmd
                     << ") popen() errno "   << myErrno
                     << "  " << std::strerror(myErrno) << std::endl;
               break;
            }
         } while(0);

         return (errSS.str());
      } // std::string  POpenRead_t::open(std::string aCmd)


        // success when 0 == errStr.size()
        // all outputs (of each command) captured into captureSS
   std::string  spinCaptureAll(std::stringstream& captureSS)
      {
         const int BUFF_SIZE = 2*1024;

         std::stringstream errSS; // capture or error
         do
         {
            if(0 == m_FILE)
            {
               errSS << "\n  ERR: POpenRead_t::spinCaptureAll(captureSS) - m_FILE closed";
               break;
            }
            size_t discardedBlankLineCount = 0;
            do
            {
               // allocate working buff in auto var, fill with nulls
               char buff[BUFF_SIZE] = { 0 };
               if(true) { for (int i=0; i<BUFF_SIZE; ++i) assert(0 == buff[i]); }

               // char * fgets ( char * str, int num, FILE * c-stream );
               // Reads characters from c-stream and stores them as a C string
               // into buff until
               //    a) (num-1) characters have been read
               //    b) a newline or
               //    c) the end-of-file is reached
               // whichever happens first.
               // A newline character makes fgets stop reading, but it is considered
               // a valid character by the function and included in the string copied
               // to str
               // A terminating null character is automatically appended after the
               // characters copied to str.
               // Notice that fgets is quite different from gets: not only fgets
               // accepts a c-stream argument, but also allows to specify the maximum
               // size of str and includes in the string any ending newline character.

               // fgets() returns buff or null when feof()
               char* stat = std::fgets(buff,      // char*
                                       BUFF_SIZE, // count - 1024
                                       m_FILE);   // c-stream
               assert((stat == buff) || (stat == 0));
               int myErrno = errno; // capture

               if( feof(m_FILE) ) { // c-stream eof detected
                  break;
               }

               // when stat is null (and ! feof(m_FILE) ),
               //   even a blank line contains "any ending newline char"
               // TBD:
               // if (0 == stat) {
               //   errSS << "0 == fgets(buff, BUFF_SIZE_1024, m_FILE) " << myErrno
               //         << "  " << std::strerror(myErrno) << std::endl;
               //   break;
               // }

               if(ferror(m_FILE)) { // file problem
                  errSS << "Err: fgets() with ferror: " << std::strerror(myErrno);
                  break;
               }

               if(strlen(buff))  captureSS << buff; // additional output
               else              discardedBlankLineCount += 1;

            }while(1);

            if(discardedBlankLineCount)
               captureSS << "\n" << "discarded blank lines: " << discardedBlankLineCount << std::endl;

         } while(0);

         return (errSS.str());

      } // std::string  POpenRead_t::spinCaptureAll(std::stringstream&  ss)

}; // class POpenRead_t

class POpenWrite_t : public POpen_t    // access to ::popen
{
public:

   POpenWrite_t(void) { }


   // ::popen(aCmd): opens a process (fork), invokes the non-interactive shell,
   //                and creates a pipe
   // returns NULL if the fork or pipe calls fail,
   //              or if it cannot allocate memory.
   // on success: 0 == return.size(), else returns error msg
   std::string  open (std::string  aCmd)
      {
         std::stringstream errSS;  // 0 == errSS.str().size() is success
         assert (aCmd.size() > 0);
         assert (0 == m_FILE);
         m_cmd = aCmd;  // capture

         do // poor man's try block
         {
            if(true) // diagnosis only
               std::cout << "\n  POpenWrite_t::open(cmd): cmd: \n  '"
                         << "'" << m_cmd << std::endl;

            m_FILE = ::popen (m_cmd.c_str(), "w"); // use m_FILE to write to sub-task std::in

            int myErrno = errno;
            if(0 == m_FILE)
            {
               errSS << "\n  POpenWrite_t::open(" << m_cmd
                     << ") popen() errno "        << myErrno
                     << "  "  << std::strerror(myErrno)  << std::endl;
               break;
            }
         } while(0);

         return (errSS.str());
      } // std::string  POpenWrite_t::open(std::string  aCmd)

   // TBR - POpenWrite_t::write(const std::string& s) 
   //           work in progress - see demo write mode
}; // class POpenWrite_t
#endif